"""
Enhanced stream manager with Redis support for shared transcoding processes.
Implements connection pooling and multi-worker coordination.
"""

import asyncio
import json
import math
import time
import uuid
import hashlib
from typing import Dict, List, Optional, Tuple, Any
import logging
from config import settings
import os
import tempfile

logger = logging.getLogger(__name__)

try:
    import redis.asyncio as redis  # noqa: F401

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logger.warning("Redis not available - falling back to single-worker mode")


async def probe_duration(
    url: str,
    user_agent: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 10.0,
) -> Optional[float]:
    """Run ffprobe to get the duration of a media source.

    Returns duration in seconds, or None if ffprobe fails or times out.
    """
    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_entries", "format=duration",
    ]

    # Pass the same auth that ffmpeg would use for network inputs
    is_network = isinstance(url, str) and "://" in url and not url.startswith("file://")
    if user_agent and is_network:
        cmd.extend(["-user_agent", user_agent])
    if headers and is_network:
        header_str = "".join([f"{k}: {v}\r\n" for k, v in headers.items()])
        cmd.extend(["-headers", header_str])

    cmd.append(url)

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)

        if proc.returncode != 0:
            logger.warning(
                f"ffprobe failed for {url}: exit code {proc.returncode}, "
                f"stderr={stderr.decode('utf-8', errors='ignore')[:200]}"
            )
            return None

        data = json.loads(stdout.decode("utf-8", errors="ignore"))
        duration_str = data.get("format", {}).get("duration")
        if duration_str:
            duration = float(duration_str)
            logger.info(f"ffprobe detected duration: {duration:.2f}s for {url}")
            return duration

        logger.warning(f"ffprobe returned no duration for {url}")
        return None

    except asyncio.TimeoutError:
        logger.warning(f"ffprobe timed out after {timeout}s for {url}")
        try:
            proc.kill()
            await proc.wait()
        except Exception:
            pass
        return None
    except Exception as e:
        logger.warning(f"ffprobe error for {url}: {e}")
        return None


class SharedTranscodingProcess:
    """Represents a shared FFmpeg transcoding process with broadcasting to multiple clients"""

    def __init__(
        self,
        stream_id: str,
        url: str,
        profile: str,
        ffmpeg_args: List[str],
        user_agent: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        hls_base_dir: Optional[str] = None,
    ):
        self.stream_id = stream_id
        self.url = url
        self.profile = profile
        self.ffmpeg_args = ffmpeg_args
        self.user_agent = user_agent
        self.headers = headers or {}
        self.metadata = metadata or {}
        # Base directory to create HLS per-stream directories in. If None,
        # the process will fall back to the system tempdir.
        self.hls_base_dir = hls_base_dir
        self.process: Optional[asyncio.subprocess.Process] = None
        self.clients: Dict[str, float] = {}  # client_id -> last_access_time
        self.created_at = time.time()
        self.last_access = time.time()
        self.total_bytes_served = 0
        self.status = "starting"

        # Broadcasting support - each client gets its own queue
        self.client_queues: Dict[str, asyncio.Queue] = {}
        self._broadcaster_task: Optional[asyncio.Task] = None
        self._broadcaster_lock = asyncio.Lock()

        self.last_chunk_time = time.time()  # Track when last chunk was produced
        self.output_timeout = 30  # Seconds without output before considering failed

        # VOD seek bar support: pre-built manifest and seek tracking
        self.source_duration: Optional[float] = None  # Duration from ffprobe (seconds)
        self.prebuilt_manifest: Optional[str] = None  # Complete VOD manifest
        self.total_segments: int = 0  # Total expected segments
        self.segment_duration: float = 6.0  # Segment length (parsed from ffmpeg args)
        self.highest_segment: int = -1  # Highest segment number on disk
        self._seek_lock = asyncio.Lock()  # Prevent concurrent seek restarts
        self._last_seek_time: float = 0.0  # Debounce rapid seeks
        self._active_waiters: int = 0  # Number of in-flight wait_for_segment calls
        # Detect output mode (stdout stream vs HLS files)
        self.mode = "stdout"
        self.hls_dir: Optional[str] = None
        self.output_dir: Optional[str] = None
        self.output_file_path: Optional[str] = None
        self.output_format = str(
            self.metadata.get("transcode_output_format", "mp4")
        ).lower()
        # If ffmpeg_args suggest HLS output, switch to hls mode
        joined_args = " ".join(self.ffmpeg_args).lower()
        transcode_delivery = str(self.metadata.get("transcode_delivery", "")).lower()
        if transcode_delivery == "file_vod":
            self.mode = "file"
        elif (
            transcode_delivery == "hls_vod"
            or "-hls_time" in joined_args
            or "-hls_list_size" in joined_args
            or "-f hls" in joined_args
        ):
            self.mode = "hls"

        if self.mode in {"hls", "file"}:
            # Determine base dir for file-based outputs
            base_dir = None
            if self.hls_base_dir:
                base_dir = self.hls_base_dir
            else:
                try:
                    base_dir = tempfile.gettempdir()
                except Exception:
                    base_dir = None

            # Ensure base dir exists if provided
            if base_dir:
                try:
                    os.makedirs(base_dir, exist_ok=True)
                except Exception:
                    pass

        if self.mode == "hls":
            # Create a per-stream directory for HLS segments
            try:
                self.hls_dir = tempfile.mkdtemp(
                    prefix=f"m3u_proxy_hls_{self.stream_id}_", dir=base_dir
                )
                # Fix #6: Set explicit permissions to ensure NGINX can read segments
                # rwxr-xr-x (755) allows owner full access, group/others can read/execute
                try:
                    os.chmod(self.hls_dir, 0o755)
                except Exception as e:
                    logger.warning(
                        f"Failed to set permissions on HLS dir {self.hls_dir}: {e}"
                    )
            except Exception as e:
                # Fallback to system tempdir without dir param
                logger.warning(
                    f"Failed to create HLS dir in {base_dir}: {e}, falling back to system tempdir"
                )
                self.hls_dir = tempfile.mkdtemp(
                    prefix=f"m3u_proxy_hls_{self.stream_id}_"
                )
                try:
                    os.chmod(self.hls_dir, 0o755)
                except Exception:
                    pass

            logger.info(
                f"SharedTranscodingProcess {self.stream_id} will run in HLS mode, hls_dir={self.hls_dir}"
            )
        elif self.mode == "file":
            try:
                self.output_dir = tempfile.mkdtemp(
                    prefix=f"m3u_proxy_file_{self.stream_id}_", dir=base_dir
                )
            except Exception as e:
                logger.warning(
                    f"Failed to create file output dir in {base_dir}: {e}, falling back to system tempdir"
                )
                self.output_dir = tempfile.mkdtemp(
                    prefix=f"m3u_proxy_file_{self.stream_id}_"
                )

            try:
                os.chmod(self.output_dir, 0o755)
            except Exception:
                pass

            extension = self.output_format or "mp4"
            self.output_file_path = os.path.join(self.output_dir, f"output.{extension}")
            self.metadata["artifact_path"] = self.output_file_path
            self.metadata["artifact_dir"] = self.output_dir
            self.metadata["artifact_content_type"] = self.output_format
            self.metadata["artifact_complete"] = "false"
            self.metadata["artifact_size"] = "0"

            logger.info(
                f"SharedTranscodingProcess {self.stream_id} will run in file mode, output={self.output_file_path}"
            )

    async def start_process(self):
        """Start the FFmpeg process"""
        try:
            logger.info(f"Starting shared FFmpeg process for stream {self.stream_id}")

            # For HLS VOD mode: probe duration and pre-build manifest for seek bar support
            if self.mode == "hls" and not self.prebuilt_manifest and not self.source_duration:
                self.segment_duration = self._parse_segment_duration()
                duration = await probe_duration(
                    self.url, self.user_agent, self.headers
                )
                if duration and duration > 0:
                    self.source_duration = duration
                    # Force HLS args compatible with VOD before building manifest
                    # so segment naming is guaranteed to match
                    self._force_vod_hls_args()
                    self.prebuilt_manifest = self._build_vod_manifest(
                        duration, self.segment_duration
                    )
                    self.total_segments = max(1, math.ceil(duration / self.segment_duration))
                    logger.info(
                        f"Pre-built VOD manifest for stream {self.stream_id}: "
                        f"duration={duration:.1f}s, segments={self.total_segments}, "
                        f"segment_duration={self.segment_duration}s"
                    )
                else:
                    logger.info(
                        f"No duration from ffprobe for stream {self.stream_id}, "
                        f"falling back to progressive manifest"
                    )

            # Build FFmpeg command - ensure output to stdout
            ffmpeg_cmd = ["ffmpeg"]

            # Add user agent / headers only for network inputs (http/rtsp/etc.)
            if (
                self.user_agent
                and isinstance(self.url, str)
                and ("://" in self.url and not self.url.startswith("file://"))
            ):
                ffmpeg_cmd.extend(["-user_agent", self.user_agent])

            # Add headers if provided, ensuring proper format and only for network inputs
            if (
                self.headers
                and isinstance(self.url, str)
                and ("://" in self.url and not self.url.startswith("file://"))
            ):
                header_str = "".join([f"{k}: {v}\r\n" for k, v in self.headers.items()])
                ffmpeg_cmd.extend(["-headers", header_str])

            # Process ffmpeg_args and insert options right before -i flag
            # For HLS inputs with extensionless segment URLs, we need special handling
            processed_args = []
            is_hls_input = isinstance(self.url, str) and self.url.lower().endswith(
                ".m3u8"
            )
            is_network_input = isinstance(self.url, str) and (
                "://" in self.url and not self.url.startswith("file://")
            )
            i = 0
            while i < len(self.ffmpeg_args):
                arg = self.ffmpeg_args[i]
                if arg == "-i" and i + 1 < len(self.ffmpeg_args):
                    # Found -i flag - inject options right before it if needed

                    # Enable reconnection for ALL network inputs (HTTP, RTSP, etc.)
                    # so transient I/O errors don't kill the transcode
                    if is_network_input:
                        processed_args.extend(
                            [
                                "-reconnect",
                                "1",
                                "-reconnect_streamed",
                                "1",
                                "-reconnect_delay_max",
                                "5",
                            ]
                        )

                    if is_hls_input:
                        # Add protocol whitelist to allow http/https URLs in playlists
                        processed_args.extend(
                            ["-protocol_whitelist", "file,http,https,tcp,tls,crypto"]
                        )
                        # Allow any extensions (including extensionless) for segments
                        processed_args.extend(["-allowed_extensions", "ALL"])
                    # Add -i flag and use self.url as the input
                    processed_args.append(arg)
                    # Use current URL (updated during failover)
                    processed_args.append(self.url)

                    # Add stream mapping if transcoding (not copy mode)
                    # Check if this is a transcoding profile (has -c:v or -c:a that's not 'copy')
                    is_transcoding = False
                    for j, cmd_arg in enumerate(self.ffmpeg_args):
                        if cmd_arg in ["-c:v", "-c:a"] and j + 1 < len(
                            self.ffmpeg_args
                        ):
                            if self.ffmpeg_args[j + 1] not in ["copy", "Copy"]:
                                is_transcoding = True
                                break

                    # Only add stream mapping for transcoding profiles
                    if is_transcoding:
                        # Check if user already specified -map
                        has_map = any(str(a) == "-map" for a in self.ffmpeg_args)
                        if not has_map:
                            # Map all video and audio streams
                            # The '?' makes streams optional - won't fail if stream type doesn't exist (e.g. audio only)
                            processed_args.extend(["-map", "0:v:0?", "-map", "0:a:0?"])
                            logger.debug(
                                "Added stream mapping for transcoding profile: -map 0:v:0? -map 0:a:0?"
                            )

                    i += 2  # Skip the old URL in ffmpeg_args
                else:
                    processed_args.append(arg)
                    i += 1

            ffmpeg_cmd.extend(processed_args)

            # If HLS mode, ensure we write to the hls_dir index.m3u8
            if self.mode == "hls":
                # If the ffmpeg args already include an output filename, respect it
                # Otherwise append the playlist target into the hls dir
                playlist_path = os.path.join(
                    self.hls_dir if self.hls_dir else tempfile.gettempdir(),
                    "index.m3u8",
                )
                # If ffmpeg_args already specify an output playlist, replace any m3u8 token with absolute path
                replaced = False
                for i, token in enumerate(ffmpeg_cmd):
                    try:
                        if not isinstance(token, str):
                            continue
                        t_lower = token.lower()
                        # Skip tokens that look like input specs ("-i <url>" or "-i<url>")
                        if t_lower.endswith(".m3u8"):
                            prev = ffmpeg_cmd[i - 1] if i > 0 else None
                            if isinstance(prev, str) and prev == "-i":
                                # This is an input URL; do NOT replace it with our output path
                                continue
                            if t_lower.startswith("-i") and t_lower[2:].endswith(
                                ".m3u8"
                            ):
                                # Token like '-ihttp://.../playlist.m3u8' - treat as input, skip
                                continue
                            # Otherwise this looks like an output playlist token, replace it
                            ffmpeg_cmd[i] = playlist_path
                            replaced = True
                    except Exception:
                        continue

                if not replaced:
                    # Remove any pipe outputs which are inappropriate for file-based HLS output
                    ffmpeg_cmd = [
                        t
                        for t in ffmpeg_cmd
                        if not (isinstance(t, str) and t.startswith("pipe:"))
                    ]
                    # Append absolute playlist path as the intended HLS output
                    ffmpeg_cmd.append(playlist_path)

            elif self.mode == "file":
                output_path = self.output_file_path or os.path.join(
                    tempfile.gettempdir(), f"{self.stream_id}.{self.output_format or 'mp4'}"
                )
                replaced = False
                for i, token in enumerate(ffmpeg_cmd):
                    try:
                        if not isinstance(token, str):
                            continue
                        t_lower = token.lower()
                        prev = ffmpeg_cmd[i - 1] if i > 0 else None
                        if isinstance(prev, str) and prev == "-i":
                            continue
                        if token == "output.file" or t_lower.startswith("pipe:") or token == "-":
                            ffmpeg_cmd[i] = output_path
                            replaced = True
                    except Exception:
                        continue

                if not replaced:
                    ffmpeg_cmd = [
                        t
                        for t in ffmpeg_cmd
                        if not (
                            isinstance(t, str)
                            and (t.startswith("pipe:") or t == "-")
                        )
                    ]
                    ffmpeg_cmd.append(output_path)
            else:
                # Ensure we're outputting to stdout in MPEGTS format only if no -f specified
                if "-f" not in [a.lower() for a in ffmpeg_cmd]:
                    ffmpeg_cmd.extend(["-f", "mpegts"])
                # Use pipe:1 for stdout-based broadcasting unless an output file is specified
                if (
                    "pipe:1" not in ffmpeg_cmd
                    and "-" not in ffmpeg_cmd
                    and self.mode == "stdout"
                ):
                    ffmpeg_cmd.append("pipe:1")

            logger.info(f"FFmpeg command: {' '.join(ffmpeg_cmd)}")

            self.process = await asyncio.create_subprocess_exec(
                *ffmpeg_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            self.status = "running"
            logger.info(f"Shared FFmpeg process started with PID: {self.process.pid}")

            # Start stderr logging task
            asyncio.create_task(self._log_stderr())

            # Start broadcaster task to read from FFmpeg and send to all clients
            if self.mode == "stdout":
                self._broadcaster_task = asyncio.create_task(self._broadcast_loop())
            else:
                # In HLS/file mode, watch file outputs to update last_chunk_time
                self._broadcaster_task = asyncio.create_task(self._file_output_watch_loop())

            return True

        except Exception as e:
            logger.error(f"Failed to start shared FFmpeg process: {e}")
            self.status = "failed"
            return False

    async def _broadcast_loop(self):
        """Read from FFmpeg stdout and broadcast to all client queues"""
        if not self.process or not self.process.stdout:
            logger.error(
                f"Cannot start broadcaster - no process or stdout for {self.stream_id}"
            )
            return

        logger.info(f"Starting broadcaster for stream {self.stream_id}")

        try:
            while self.process and self.process.returncode is None:
                # Read chunk from FFmpeg
                chunk = await self.process.stdout.read(32768)
                if not chunk:
                    logger.info(f"FFmpeg stdout closed for stream {self.stream_id}")
                    break

                # Update last chunk time
                self.last_chunk_time = time.time()

                # Broadcast to all client queues
                async with self._broadcaster_lock:
                    dead_clients = []
                    for client_id, queue in self.client_queues.items():
                        try:
                            # Use put_nowait to avoid blocking if a client's queue is full
                            queue.put_nowait(chunk)
                        except asyncio.QueueFull:
                            # When the queue is full, remove the oldest chunk to make space
                            try:
                                queue.get_nowait()
                            except asyncio.QueueEmpty:
                                # Should not happen if QueueFull was just raised, but defensive
                                pass
                            # Retry putting the new chunk
                            try:
                                queue.put_nowait(chunk)
                            except asyncio.QueueFull:
                                logger.warning(
                                    f"Client {client_id} queue full after dropping chunk"
                                )
                        except Exception as e:
                            logger.error(f"Error sending to client {client_id}: {e}")
                            dead_clients.append(client_id)

                    # Remove dead clients
                    for client_id in dead_clients:
                        self.client_queues.pop(client_id, None)

                # Update stats
                self.total_bytes_served += len(chunk)
                self.last_access = time.time()

        except Exception as e:
            logger.error(f"Broadcaster error for stream {self.stream_id}: {e}")
        finally:
            logger.info(f"Broadcaster stopped for stream {self.stream_id}")
            # Signal all clients that the stream has ended
            async with self._broadcaster_lock:
                for queue in self.client_queues.values():
                    try:
                        queue.put_nowait(None)  # None signals end of stream
                    except Exception:
                        pass

    async def _hls_watch_loop(self):
        """Watch HLS output directory and update last_chunk_time when new segments appear"""
        if not self.hls_dir:
            return
        try:
            known = set()
            while self.process and self.process.returncode is None:
                # list files
                try:
                    files = os.listdir(self.hls_dir)
                except FileNotFoundError:
                    files = []

                new_files = [f for f in files if f not in known]
                if new_files:
                    self.last_chunk_time = time.time()
                    for f in new_files:
                        known.add(f)
                        # Track highest segment number for seek decisions
                        if f.startswith("segment_") and f.endswith(".ts"):
                            try:
                                seg_num = int(f.split("_")[1].split(".")[0])
                                if seg_num > self.highest_segment:
                                    self.highest_segment = seg_num
                            except (IndexError, ValueError):
                                pass
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.debug(f"HLS watch loop ended for {self.stream_id}: {e}")

    async def _file_output_watch_loop(self):
        """Watch HLS or file outputs and update last_chunk_time when they grow."""
        if self.mode == "hls":
            await self._hls_watch_loop()
            return

        if self.mode != "file" or not self.output_file_path:
            return

        try:
            last_size = -1
            stable_since = None
            while self.process and self.process.returncode is None:
                try:
                    if os.path.exists(self.output_file_path):
                        current_size = os.path.getsize(self.output_file_path)
                        if current_size != last_size:
                            self.last_chunk_time = time.time()
                            self.metadata["artifact_size"] = str(current_size)
                            self.metadata["artifact_last_growth_at"] = str(
                                self.last_chunk_time
                            )
                            stable_since = time.time()
                            last_size = current_size
                        elif current_size > 0 and stable_since is not None:
                            self.metadata["artifact_stable_for"] = str(
                                max(0.0, time.time() - stable_since)
                            )
                except OSError:
                    pass
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.debug(f"File watch loop ended for {self.stream_id}: {e}")
        finally:
            if (
                self.mode == "file"
                and self.output_file_path
                and os.path.exists(self.output_file_path)
            ):
                try:
                    self.metadata["artifact_size"] = str(
                        os.path.getsize(self.output_file_path)
                    )
                except OSError:
                    pass
                if self.process and self.process.returncode == 0:
                    self.metadata["artifact_complete"] = "true"

    async def _log_stderr(self):
        """Log FFmpeg stderr output and monitor for write errors and input failures"""
        if not self.process or not self.process.stderr:
            return

        try:
            # Monitor FFmpeg stderr for various error conditions
            write_error_patterns = [
                "no space left on device",
                "permission denied",
                "i/o error",
                "disk full",
                "cannot write",
                "failed to open",
                "error writing",
            ]

            # Input/connection error patterns that should trigger failover
            input_error_patterns = [
                "error opening input",
                "failed to resolve hostname",
                "connection refused",
                "connection timed out",
                "input/output error",
                "server returned 4",  # Matches 403, 404, etc.
                "server returned 5",  # Matches 500, 502, 503, etc.
                "invalid data found",
                "protocol not found",
                "end of file",
            ]

            # Read stderr in small chunks and buffer lines ourselves to avoid
            # asyncio.StreamReader's LimitOverrunError when ffmpeg writes very
            # long lines without a newline (which results in the message
            # "Separator is not found, and chunk exceed the limit"). This keeps
            # the stderr reader alive instead of letting an exception kill the
            # task and potentially lead to the stream being cleaned up later.
            buf = b""
            CHUNK_SIZE = 4096
            MAX_BUFFER = 10 * 1024 * 1024  # 10 MB

            while self.process and self.process.returncode is None:
                # Read a small chunk from stderr; this will never raise
                # LimitOverrunError because we are not using readline/readuntil.
                chunk = await self.process.stderr.read(CHUNK_SIZE)
                if not chunk:
                    break

                buf += chunk

                # Split on newline and process full lines
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line_str = line.decode("utf-8", errors="ignore").strip()
                    if not line_str:
                        continue

                    # Log FFmpeg output (you could parse stats here)
                    logger.debug(f"FFmpeg [{self.stream_id}]: {line_str}")

                    line_lower = line_str.lower()

                    # Check for write errors
                    for pattern in write_error_patterns:
                        if pattern in line_lower:
                            logger.error(
                                f"FFmpeg write error detected for {self.stream_id}: {line_str}"
                            )
                            # Mark stream as failed to trigger cleanup
                            self.status = "failed"
                            break

                    # Check for input/connection errors that should trigger failover
                    for pattern in input_error_patterns:
                        if pattern in line_lower:
                            logger.error(
                                f"FFmpeg input error detected for {self.stream_id}: {line_str}"
                            )
                            # Mark stream as failed due to input error
                            self.status = "input_failed"
                            break

                # Safety: prevent the buffer from growing without bound
                if len(buf) > MAX_BUFFER:
                    logger.warning(
                        f"Stderr buffer exceeded {MAX_BUFFER} bytes for {self.stream_id}, truncating"
                    )
                    buf = b""

        except Exception as e:
            logger.error(f"Error reading FFmpeg stderr for {self.stream_id}: {e}")

    async def read_playlist(self) -> Optional[str]:
        """Read the HLS playlist. Returns the pre-built VOD manifest when available,
        otherwise falls back to reading ffmpeg's progressive manifest from disk."""
        # If we have a pre-built manifest (from ffprobe duration), always use it
        if self.prebuilt_manifest:
            return self.prebuilt_manifest

        if not self.hls_dir:
            return None
        playlist_path = os.path.join(self.hls_dir, "index.m3u8")
        try:
            if os.path.exists(playlist_path):
                with open(playlist_path, "r", encoding="utf-8", errors="ignore") as fh:
                    return fh.read()
        except Exception as e:
            logger.error(f"Error reading playlist {playlist_path}: {e}")
        return None

    def get_segment_path(self, segment_name: str) -> Optional[str]:
        if not self.hls_dir:
            return None
        candidate = os.path.join(self.hls_dir, segment_name)
        if os.path.exists(candidate):
            return candidate
        return None

    def _parse_segment_duration(self) -> float:
        """Parse -hls_time value from ffmpeg_args, default 6.0."""
        for i, arg in enumerate(self.ffmpeg_args):
            if arg == "-hls_time" and i + 1 < len(self.ffmpeg_args):
                try:
                    return float(self.ffmpeg_args[i + 1])
                except (ValueError, IndexError):
                    pass
        return 6.0

    def _build_vod_manifest(self, duration_seconds: float, segment_duration: float) -> str:
        """Build a complete VOD HLS manifest for the given duration.

        The manifest lists all segments upfront with #EXT-X-ENDLIST so players
        show a seek bar from the start, even before ffmpeg has produced every segment.
        """
        total_segments = max(1, math.ceil(duration_seconds / segment_duration))
        target_duration = math.ceil(segment_duration)

        lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            f"#EXT-X-TARGETDURATION:{target_duration}",
            "#EXT-X-MEDIA-SEQUENCE:0",
            "#EXT-X-PLAYLIST-TYPE:VOD",
        ]

        remaining = duration_seconds
        for i in range(total_segments):
            seg_dur = min(segment_duration, remaining)
            # Ensure last segment has a positive duration
            if seg_dur <= 0:
                seg_dur = segment_duration
            lines.append(f"#EXTINF:{seg_dur:.6f},")
            lines.append(f"segment_{i:06d}.ts")
            remaining -= segment_duration

        lines.append("#EXT-X-ENDLIST")
        lines.append("")  # trailing newline

        return "\n".join(lines)

    def _force_vod_hls_args(self):
        """Override HLS-related ffmpeg args to ensure all segments stay on disk.

        When a pre-built VOD manifest is active, we need:
        - ``-hls_list_size 0`` so ffmpeg doesn't rotate/delete old segments
        - ``-hls_playlist_type vod`` so ffmpeg writes a proper VOD playlist on disk
        - ``-hls_segment_filename`` as absolute path so segments land in hls_dir
        - ``-hls_flags delete_segments`` removed if present (prevents segment deletion)
        """
        # Build absolute segment filename — ffmpeg resolves relative paths
        # against its CWD, NOT the output file directory
        segment_pattern = os.path.join(
            self.hls_dir or "", "segment_%06d.ts"
        )

        new_args = []
        i = 0
        while i < len(self.ffmpeg_args):
            arg = self.ffmpeg_args[i]
            if arg == "-hls_list_size" and i + 1 < len(self.ffmpeg_args):
                # Replace with 0 (keep all segments)
                new_args.extend(["-hls_list_size", "0"])
                i += 2
                continue
            if arg == "-hls_playlist_type" and i + 1 < len(self.ffmpeg_args):
                # Replace with vod
                new_args.extend(["-hls_playlist_type", "vod"])
                i += 2
                continue
            if arg == "-hls_segment_filename" and i + 1 < len(self.ffmpeg_args):
                # Force absolute path with 6-digit naming to match pre-built manifest
                new_args.extend(["-hls_segment_filename", segment_pattern])
                i += 2
                continue
            if arg == "-hls_flags" and i + 1 < len(self.ffmpeg_args):
                # Remove delete_segments from flags if present
                flags = self.ffmpeg_args[i + 1]
                cleaned = "+".join(
                    f for f in flags.split("+") if f != "delete_segments"
                )
                if cleaned:
                    new_args.extend(["-hls_flags", cleaned])
                # else: drop -hls_flags entirely if only delete_segments was set
                i += 2
                continue
            new_args.append(arg)
            i += 1

        # Add missing args if they weren't in the original command
        has_list_size = any(a == "-hls_list_size" for a in new_args)
        has_playlist_type = any(a == "-hls_playlist_type" for a in new_args)
        has_segment_filename = any(a == "-hls_segment_filename" for a in new_args)

        if not has_list_size:
            new_args.insert(-1, "-hls_list_size")
            new_args.insert(-1, "0")
        if not has_playlist_type:
            new_args.insert(-1, "-hls_playlist_type")
            new_args.insert(-1, "vod")
        if not has_segment_filename:
            new_args.insert(-1, "-hls_segment_filename")
            new_args.insert(-1, segment_pattern)

        self.ffmpeg_args = new_args

    async def wait_for_segment(
        self,
        segment_name: str,
        timeout: float = 60.0,
        heartbeat_callback=None,
    ) -> Optional[str]:
        """Wait for a segment to appear on disk, seeking ffmpeg forward if needed.

        Args:
            segment_name: The segment filename (e.g. segment_000042.ts).
            timeout: Maximum seconds to wait.
            heartbeat_callback: Optional callable invoked each poll iteration so
                callers can keep their own timeouts (e.g. client last_access) fresh.

        Returns the absolute file path once available, or None on timeout.
        """
        if not self.hls_dir:
            return None

        segment_path = os.path.join(self.hls_dir, segment_name)

        # Already on disk — fast path
        if os.path.exists(segment_path):
            return segment_path

        # Not a seekable VOD stream — no point waiting
        if not self.prebuilt_manifest:
            return None

        # Extract segment number
        try:
            seg_num = int(segment_name.split("_")[1].split(".")[0])
        except (IndexError, ValueError):
            return None

        # Track that a segment wait is in progress so cleanup doesn't kill us
        self._active_waiters += 1
        try:
            # Decide: wait for ffmpeg to catch up, or seek ahead
            SEEK_THRESHOLD = 3  # segments
            if self.highest_segment >= 0 and seg_num > self.highest_segment + SEEK_THRESHOLD:
                # Segment is far ahead — restart ffmpeg at the target position
                await self._seek_to_segment(seg_num)

            # Poll for the segment to appear
            poll_interval = 0.3
            waited = 0.0
            while waited < timeout:
                if os.path.exists(segment_path):
                    # Wait briefly for ffmpeg to finish writing the segment
                    await asyncio.sleep(0.1)
                    return segment_path

                # If ffmpeg has exited and the segment still doesn't exist, give up
                if self.process and self.process.returncode is not None:
                    return None

                # Keep the process alive — update timestamps so cleanup loops
                # don't consider this stream stale while we're actively waiting
                self.last_access = time.time()
                self.last_chunk_time = time.time()

                # Let the caller refresh its own timeouts (e.g. client last_access)
                if heartbeat_callback:
                    try:
                        heartbeat_callback()
                    except Exception:
                        pass

                await asyncio.sleep(poll_interval)
                waited += poll_interval

            logger.warning(
                f"Timed out waiting for segment {segment_name} after {timeout}s "
                f"(highest_segment={self.highest_segment}, stream={self.stream_id})"
            )
            return None
        finally:
            self._active_waiters -= 1

    async def _seek_to_segment(self, target_segment: int):
        """Restart ffmpeg with -ss to transcode from a specific position."""
        async with self._seek_lock:
            # Debounce: don't restart if we just did
            if time.time() - self._last_seek_time < 2.0:
                logger.debug(
                    f"Skipping seek to segment {target_segment}: debounce (last seek {time.time() - self._last_seek_time:.1f}s ago)"
                )
                return

            # If the segment is already on disk (maybe another seek produced it), skip
            expected_file = os.path.join(self.hls_dir, f"segment_{target_segment:06d}.ts")
            if os.path.exists(expected_file):
                return

            seek_seconds = max(0, target_segment * self.segment_duration - self.segment_duration)
            logger.info(
                f"Seeking ffmpeg to segment {target_segment} (t={seek_seconds:.1f}s) for stream {self.stream_id}"
            )

            # 1. Kill current ffmpeg process
            if self.process and self.process.returncode is None:
                try:
                    self.process.terminate()
                    await asyncio.wait_for(self.process.wait(), timeout=3.0)
                except asyncio.TimeoutError:
                    self.process.kill()
                    await self.process.wait()
                except Exception as e:
                    logger.warning(f"Error stopping ffmpeg for seek: {e}")

            # 2. Cancel the existing watch loop
            if self._broadcaster_task and not self._broadcaster_task.done():
                self._broadcaster_task.cancel()
                try:
                    await self._broadcaster_task
                except asyncio.CancelledError:
                    pass

            # 3. Rebuild ffmpeg args with -ss, -output_ts_offset, and -start_number
            #    Strip any existing seek-related flags from prior seeks
            new_args = []
            i = 0
            while i < len(self.ffmpeg_args):
                arg = self.ffmpeg_args[i]
                if arg in ("-ss", "-start_number", "-output_ts_offset") and i + 1 < len(self.ffmpeg_args):
                    # Strip previous seek flags (will re-insert below)
                    i += 2
                elif arg == "-i" and i + 1 < len(self.ffmpeg_args):
                    # Insert -ss before -i for fast input seeking
                    new_args.extend(["-ss", f"{seek_seconds:.3f}"])
                    new_args.append(arg)
                    new_args.append(self.ffmpeg_args[i + 1])
                    # Insert -output_ts_offset AFTER -i so output timestamps
                    # match the seek position (without this, -ss resets PTS to 0
                    # and the player's seek bar breaks)
                    new_args.extend(["-output_ts_offset", f"{seek_seconds:.3f}"])
                    i += 2
                else:
                    new_args.append(arg)
                    i += 1

            # Insert -start_number so segment filenames match the pre-built manifest
            # Find a good spot: right before -hls_segment_filename or at the end of HLS flags
            insert_idx = None
            for idx, a in enumerate(new_args):
                if a == "-hls_segment_filename":
                    insert_idx = idx
                    break
            if insert_idx is not None:
                new_args.insert(insert_idx, str(target_segment))
                new_args.insert(insert_idx, "-start_number")
            else:
                # Append before the output filename (last arg)
                new_args.insert(-1, "-start_number")
                new_args.insert(-1, str(target_segment))

            # 4. Swap args and restart
            original_args = self.ffmpeg_args
            self.ffmpeg_args = new_args
            self.status = "running"
            self._last_seek_time = time.time()

            success = await self.start_process()
            if not success:
                logger.error(f"Failed to restart ffmpeg after seek for stream {self.stream_id}")
                # Restore original args so a retry can try again
                self.ffmpeg_args = original_args

    async def add_client(self, client_id: str) -> asyncio.Queue:
        """Add a client to this shared process and return their queue"""
        async with self._broadcaster_lock:
            # Create a queue for this client (max 100 chunks buffered)
            client_queue = asyncio.Queue(maxsize=settings.CHANGE_BUFFER_CHUNKS)
            self.client_queues[client_id] = client_queue
            self.clients[client_id] = time.time()
            self.last_access = time.time()
            logger.info(
                f"Client {client_id} joined shared stream {self.stream_id} ({len(self.clients)} total)"
            )
            return client_queue

    async def remove_client(self, client_id: str):
        """Remove a client from this shared process"""
        async with self._broadcaster_lock:
            if client_id in self.clients:
                del self.clients[client_id]
                self.last_access = time.time()
                logger.info(
                    f"Client {client_id} left shared stream {self.stream_id} ({len(self.clients)} remaining)"
                )

            # Remove client's queue
            if client_id in self.client_queues:
                del self.client_queues[client_id]

    async def prune_stale_clients(self, timeout: int):
        """Remove clients that have been inactive for a while"""
        stale_clients = [
            cid
            for cid, last_seen in self.clients.items()
            if time.time() - last_seen > timeout
        ]
        for client_id in stale_clients:
            await self.remove_client(client_id)

    def should_cleanup(self, timeout: int = 300) -> bool:
        """Check if this process should be cleaned up (no clients for timeout seconds)"""
        # Never clean up while segment requests are actively waiting for transcoding
        if self._active_waiters > 0:
            return False
        return not self.clients and (time.time() - self.last_access > timeout)

    def health_check(self):
        """Check the health of the FFmpeg process."""
        if self.process and self.process.returncode is not None:
            if self.status != "failed":
                logger.warning(
                    f"FFmpeg process for stream {self.stream_id} has exited with code {self.process.returncode}."
                )
                self.status = "failed"
                return False

        # Also check if process exists but is not responding
        if self.process is None and self.status == "running":
            logger.warning(
                f"FFmpeg process for stream {self.stream_id} is None but status is running"
            )
            self.status = "failed"
            return False

        # Check if no output for too long (indicates stuck process)
        if time.time() - self.last_chunk_time > self.output_timeout:
            logger.warning(
                f"FFmpeg process for stream {self.stream_id} has produced no output for {self.output_timeout}s, marking as failed"
            )
            self.status = "failed"
            return False

        return self.status == "running" and self.process is not None

    async def cleanup(self):
        """Clean up the FFmpeg process and any output artifacts"""
        # Fix #5: Improved cleanup with better error handling and logging
        try:
            if self.process and self.process.returncode is None:
                logger.info(
                    f"Terminating shared FFmpeg process for stream {self.stream_id}"
                )
                try:
                    self.process.terminate()
                    await asyncio.wait_for(self.process.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning(
                        "FFmpeg process didn't terminate cleanly, killing it"
                    )
                    self.process.kill()
                    await self.process.wait()
                except Exception as e:
                    logger.error(f"Error cleaning up FFmpeg process: {e}")

            self.status = "stopped"
            self.clients.clear()
        finally:
            # Always attempt artifact cleanup in finally block to ensure it runs
            await self._cleanup_hls_directory()
            await self._cleanup_file_output_directory()

    async def _cleanup_hls_directory(self):
        """Clean up HLS directory and all segments"""
        if not self.hls_dir or not os.path.isdir(self.hls_dir):
            return

        try:
            # Count files for logging
            files = os.listdir(self.hls_dir)
            file_count = len(files)

            # Remove all files in the directory
            removed_count = 0
            failed_count = 0
            for fname in files:
                try:
                    file_path = os.path.join(self.hls_dir, fname)
                    os.remove(file_path)
                    removed_count += 1
                except Exception as e:
                    failed_count += 1
                    logger.warning(f"Failed to remove HLS file {fname}: {e}")

            # Try to remove the directory itself
            try:
                os.rmdir(self.hls_dir)
                logger.info(
                    f"Cleaned up HLS directory for {self.stream_id}: removed {removed_count}/{file_count} files"
                )
            except OSError as e:
                # Directory not empty or other error
                logger.warning(
                    f"Failed to remove HLS directory {self.hls_dir}: {e} ({failed_count} files failed to delete)"
                )
            except Exception as e:
                logger.error(
                    f"Unexpected error removing HLS directory {self.hls_dir}: {e}"
                )

        except Exception as e:
            logger.error(f"Error cleaning up HLS directory for {self.stream_id}: {e}")

    async def _cleanup_file_output_directory(self):
        """Clean up file_vod output artifact and directory."""
        if not self.output_dir or not os.path.isdir(self.output_dir):
            return

        try:
            files = os.listdir(self.output_dir)
            file_count = len(files)
            removed_count = 0
            failed_count = 0

            for fname in files:
                try:
                    os.remove(os.path.join(self.output_dir, fname))
                    removed_count += 1
                except Exception as e:
                    failed_count += 1
                    logger.warning(f"Failed to remove file_vod artifact {fname}: {e}")

            try:
                os.rmdir(self.output_dir)
                logger.info(
                    f"Cleaned up file_vod directory for {self.stream_id}: removed {removed_count}/{file_count} files"
                )
            except OSError as e:
                logger.warning(
                    f"Failed to remove file_vod directory {self.output_dir}: {e} ({failed_count} files failed to delete)"
                )
            except Exception as e:
                logger.error(
                    f"Unexpected error removing file_vod directory {self.output_dir}: {e}"
                )
        except Exception as e:
            logger.error(
                f"Error cleaning up file_vod directory for {self.stream_id}: {e}"
            )


class PooledStreamManager:
    """Stream manager with Redis support and connection pooling"""

    def __init__(
        self,
        redis_url: Optional[str] = None,
        worker_id: Optional[str] = None,
        enable_sharing: bool = True,
    ):

        self.redis_url = redis_url or "redis://localhost:6379/0"
        self.worker_id = worker_id or str(uuid.uuid4())[:8]
        self.enable_sharing = enable_sharing and REDIS_AVAILABLE

        # Redis client
        # Use Any to avoid type issues when Redis not available
        self.redis_client: Optional[Any] = None

        # Event manager (set by stream manager)
        self.event_manager = None
        self.parent_stream_manager = None

        # Local process management
        self.shared_processes: Dict[str, SharedTranscodingProcess] = {}
        # client_id -> stream_id mapping
        self.client_streams: Dict[str, str] = {}
        # stream_key -> stream_id mapping (for event emission)
        self.stream_key_to_id: Dict[str, str] = {}

        # Configuration
        # seconds - how often to run cleanup loop
        self.cleanup_interval = int(getattr(settings, "CLEANUP_INTERVAL", 60))
        # seconds - Redis worker heartbeat
        self.heartbeat_interval = int(getattr(settings, "HEARTBEAT_INTERVAL", 30))
        # seconds - fallback timeout for streams with no clients
        self.stream_timeout = int(getattr(settings, "STREAM_TIMEOUT", 300))
        # Default 30 seconds - timeout for inactive clients
        self.client_timeout = int(getattr(settings, "CLIENT_TIMEOUT", 30))
        # HLS GC configuration (defaults from config.settings)
        self.hls_gc_enabled = bool(getattr(settings, "HLS_GC_ENABLED", True))
        # How often to scan filesystem for stale HLS dirs (seconds)
        self.hls_gc_interval = int(getattr(settings, "HLS_GC_INTERVAL", 600))
        # Age threshold for removing HLS dirs (seconds)
        self.hls_gc_age_threshold = int(
            getattr(settings, "HLS_GC_AGE_THRESHOLD", 60 * 60)
        )
        # Track last time GC ran so we can respect hls_gc_interval cadence
        self._last_hls_gc_run = 0
        # Base directory for HLS per-stream output. If settings.HLS_TEMP_DIR is not set,
        # fall back to the system tempdir used by tempfile.gettempdir()
        self.hls_base_dir = (
            getattr(settings, "HLS_TEMP_DIR", None) or tempfile.gettempdir()
        )

        # Tasks
        self._cleanup_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._running = False

    def get_process_by_stream_id(self, stream_id: str) -> Optional[SharedTranscodingProcess]:
        """Look up a SharedTranscodingProcess by its external stream_id."""
        for key, sid in self.stream_key_to_id.items():
            if sid == stream_id and key in self.shared_processes:
                return self.shared_processes[key]
        # Fallback: stream_key might be the stream_id itself
        return self.shared_processes.get(stream_id)

    def set_event_manager(self, event_manager):
        """Set the event manager for emitting events"""
        self.event_manager = event_manager

    def set_parent_stream_manager(self, stream_manager):
        """Set the parent stream manager for accessing stream info"""
        self.parent_stream_manager = stream_manager

    async def _emit_event(self, event_type: str, stream_id: str, data: dict):
        """Helper method to emit events if event manager is available"""
        if self.event_manager:
            try:
                from models import StreamEvent, EventType

                event = StreamEvent(
                    event_type=getattr(EventType, event_type),
                    stream_id=stream_id,
                    data=data,
                )
                await self.event_manager.emit_event(event)
            except Exception as e:
                logger.error(f"Error emitting event: {e}")

    async def start(self):
        """Start the pooled stream manager"""
        self._running = True

        if self.enable_sharing and REDIS_AVAILABLE:
            try:
                # Import here to avoid issues if redis not installed
                import redis.asyncio as redis_async

                self.redis_client = redis_async.from_url(
                    self.redis_url, decode_responses=True
                )
                await self.redis_client.ping()
                logger.info(f"Redis connected for worker {self.worker_id}")

                # Start heartbeat task
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

            except Exception as e:
                logger.warning(
                    f"Failed to connect to Redis: {e}. Running in single-worker mode"
                )
                self.enable_sharing = False
                self.redis_client = None

        # Start cleanup task
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

        mode = "multi-worker with Redis" if self.enable_sharing else "single-worker"
        logger.info(f"Pooled stream manager started in {mode} mode")

    async def stop(self):
        """Stop the pooled stream manager"""
        self._running = False

        # Cancel tasks
        if self._cleanup_task:
            self._cleanup_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

        # Clean up all local processes
        for process in list(self.shared_processes.values()):
            await process.cleanup()
        self.shared_processes.clear()

        # Close Redis connection
        if self.redis_client:
            await self._cleanup_worker_streams()
            await self.redis_client.close()

        logger.info("Pooled stream manager stopped")

    async def _heartbeat_loop(self):
        """Send periodic heartbeat to Redis"""
        while self._running:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                if self.redis_client:
                    now = time.time()
                    # Update heartbeat score
                    await self.redis_client.zadd(
                        "worker_heartbeats", {self.worker_id: now}
                    )

                    # Update worker data
                    worker_data = {
                        "last_seen": now,
                        "streams": list(self.shared_processes.keys()),
                        "worker_id": self.worker_id,
                    }
                    await self.redis_client.hset(
                        "workers", self.worker_id, json.dumps(worker_data)
                    )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")

    async def _cleanup_loop(self):
        """Periodic cleanup of stale streams and processes"""
        while self._running:
            try:
                await asyncio.sleep(self.cleanup_interval)
                await self._cleanup_stale_processes()

                # Optionally run HLS temp-dir GC (scans system temp dir for leftover m3u_proxy_hls_*)
                if self.hls_gc_enabled:
                    now = time.time()
                    if (
                        now - getattr(self, "_last_hls_gc_run", 0)
                        >= self.hls_gc_interval
                    ):
                        await self._gc_hls_temp_dirs()
                        self._last_hls_gc_run = now

                if self.enable_sharing and self.redis_client:
                    await self._cleanup_stale_redis_streams()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup error: {e}")

    async def _cleanup_stale_processes(self):
        """Clean up local processes with no clients"""
        to_cleanup = []

        for stream_id, process in self.shared_processes.items():
            process.health_check()
            await process.prune_stale_clients(self.client_timeout)
            if (
                process.should_cleanup(self.stream_timeout)
                or process.status == "failed"
            ):
                to_cleanup.append(stream_id)

        for stream_id in to_cleanup:
            logger.info(f"Cleaning up stale process for stream {stream_id}")
            await self._cleanup_local_process(stream_id)

    async def _cleanup_local_process(self, stream_key: str, emit_event: bool = False):
        """
        Clean up a specific local process.

        Args:
            stream_key: The key identifying the transcoding process
            emit_event: Whether to emit stream_stopped event (default: False)
                       Set to True only when this is the sole cleanup point
        """
        if stream_key in self.shared_processes:
            process = self.shared_processes.pop(stream_key)
            await process.cleanup()

            # Only emit event if explicitly requested
            # Normally, the parent StreamManager handles event emission
            if emit_event:
                stream_id = self.stream_key_to_id.get(stream_key, stream_key)
                await self._emit_event(
                    "STREAM_STOPPED",
                    stream_id,
                    {"reason": "transcoding_cleanup", "stream_key": stream_key},
                )

            # Clean up the mapping
            if stream_key in self.stream_key_to_id:
                del self.stream_key_to_id[stream_key]

            # Update Redis
            if self.redis_client:
                redis_key = f"stream:{stream_key}"
                await self.redis_client.delete(redis_key)
                await self.redis_client.srem(
                    f"worker:{self.worker_id}:streams", redis_key
                )

    async def _cleanup_stale_redis_streams(self):
        """Clean up stale streams from Redis (dead workers)"""
        if not self.redis_client:
            return

        try:
            # Find stale workers
            stale_threshold = time.time() - (self.heartbeat_interval * 3)
            stale_workers = await self.redis_client.zrangebyscore(
                "worker_heartbeats", -1, stale_threshold
            )

            if not stale_workers:
                return

            # Clean up streams from stale workers
            for worker_id in stale_workers:
                worker_streams_key = f"worker:{worker_id}:streams"
                stream_keys = await self.redis_client.smembers(worker_streams_key)
                if stream_keys:
                    await self.redis_client.delete(*stream_keys)
                    logger.info(
                        f"Cleaned up {len(stream_keys)} streams for stale worker {worker_id}"
                    )
                await self.redis_client.delete(worker_streams_key)

            # Remove stale workers from heartbeats and data
            await self.redis_client.zremrangebyscore(
                "worker_heartbeats", -1, stale_threshold
            )
            await self.redis_client.hdel("workers", *stale_workers)
            logger.info(f"Removed stale workers: {stale_workers}")

        except Exception as e:
            logger.error(f"Error cleaning up stale Redis streams: {e}")

    async def _cleanup_worker_streams(self):
        """Clean up streams owned by this worker from Redis"""
        if not self.redis_client:
            return

        try:
            worker_streams_key = f"worker:{self.worker_id}:streams"
            stream_keys = await self.redis_client.smembers(worker_streams_key)
            if stream_keys:
                await self.redis_client.delete(*stream_keys)
            await self.redis_client.delete(worker_streams_key)
        except Exception as e:
            logger.error(f"Error cleaning up worker streams from Redis: {e}")

    async def _gc_hls_temp_dirs(self):
        """Scan the HLS temp dir for leftover HLS directories and remove stale ones.

        Fix #3: Improved GC to avoid race conditions with active streams
        Fix #4: Only delete empty directories to avoid conflicts with FFmpeg's own segment deletion

        We look for directories created with the prefix 'm3u_proxy_hls_' and remove them if they
        are not currently in use by any active SharedTranscodingProcess and meet cleanup criteria.
        """
        try:
            # Use configured HLS base dir (may be system tempdir by default)
            tmpdir = getattr(self, "hls_base_dir", None) or tempfile.gettempdir()
            prefix = "m3u_proxy_hls_"
            now = time.time()

            # Build a set of active hls dirs to avoid deleting currently-used ones
            active_dirs = set()
            for p in self.shared_processes.values():
                if p.hls_dir:
                    active_dirs.add(os.path.abspath(p.hls_dir))

            # Iterate entries in tmpdir
            try:
                entries = os.listdir(tmpdir)
            except Exception as e:
                logger.warning(f"Failed to list HLS temp directory {tmpdir}: {e}")
                entries = []

            removed = 0
            skipped_active = 0
            skipped_not_empty = 0
            skipped_too_young = 0

            for entry in entries:
                if not entry.startswith(prefix):
                    continue

                full_path = os.path.abspath(os.path.join(tmpdir, entry))
                if not os.path.isdir(full_path):
                    continue

                # Skip any dir that's currently active
                if full_path in active_dirs:
                    skipped_active += 1
                    continue

                # Fix #4: Only delete EMPTY directories
                # FFmpeg handles segment deletion via -hls_delete_threshold
                # We only clean up directories that are completely empty (stream ended)
                try:
                    dir_contents = os.listdir(full_path)
                    if dir_contents:
                        # Directory not empty - skip it
                        # FFmpeg is still managing this directory or cleanup failed
                        skipped_not_empty += 1
                        continue
                except Exception as e:
                    logger.warning(f"Failed to check contents of {full_path}: {e}")
                    continue

                # Fix #3: Check age to avoid deleting recently-created directories
                # Use mtime for empty directories (they won't be updated anymore)
                try:
                    mtime = os.path.getmtime(full_path)
                except Exception as e:
                    logger.warning(f"Failed to get mtime for {full_path}: {e}")
                    continue

                age = now - mtime
                if age > self.hls_gc_age_threshold:
                    # Directory is empty and old enough - safe to remove
                    try:
                        # Use rmdir instead of rmtree for safety (only works on empty dirs)
                        os.rmdir(full_path)
                        removed += 1
                        logger.info(
                            f"Removed empty stale HLS dir: {full_path} (age: {int(age)}s)"
                        )
                    except OSError as e:
                        # Directory not empty or permission error
                        logger.warning(f"Failed to remove HLS dir {full_path}: {e}")
                    except Exception as e:
                        logger.error(
                            f"Unexpected error removing HLS dir {full_path}: {e}"
                        )
                else:
                    skipped_too_young += 1

            if removed or skipped_active or skipped_not_empty:
                logger.info(
                    f"HLS GC scan complete: removed {removed} empty dirs, "
                    f"skipped {skipped_active} active, {skipped_not_empty} non-empty, "
                    f"{skipped_too_young} too young (from {tmpdir})"
                )

        except Exception as e:
            logger.error(f"Error while running HLS GC: {e}")

    def _generate_stream_key(
        self,
        url: str,
        profile: str,
        ffmpeg_args: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Generate a consistent key for stream sharing.

        The key includes the detected output mode (hls / file / direct) so
        that two transcoded streams with the same URL and profile name but
        different delivery formats (e.g. MPEGTS pipe vs HLS segments) are
        never incorrectly pooled together.  This matters when both profiles
        resolve to the same name (e.g. "custom" for user-provided templates).
        """
        from stream_manager import StreamManager

        output_mode = StreamManager._detect_output_mode(ffmpeg_args, metadata)
        data = f"{url}|{profile}|{output_mode}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    async def get_or_create_shared_stream(
        self,
        url: str,
        profile: str,
        ffmpeg_args: List[str],
        client_id: str,
        user_agent: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        stream_id: Optional[str] = None,
        reuse_stream_key: Optional[str] = None,
    ) -> Tuple[str, SharedTranscodingProcess]:
        """Get existing shared stream or create new one

        Args:
            reuse_stream_key: If provided, reuse this stream key instead of generating a new one.
                              This is useful for failover scenarios where we want to maintain the
                              same stream key even though the URL has changed.
        """

        stream_key = reuse_stream_key or self._generate_stream_key(
            url, profile, ffmpeg_args=ffmpeg_args, metadata=metadata
        )

        # Track the stream_id -> stream_key mapping for event emission
        if stream_id:
            self.stream_key_to_id[stream_key] = stream_id

        # First check if we have it locally
        if stream_key in self.shared_processes:
            process = self.shared_processes[stream_key]

            # Check if the process is still healthy before reusing it
            process.health_check()

            # Check if URL has changed (failover scenario) or if process has failed
            url_changed = process.url != url
            process_failed = process.status in ("failed", "input_failed") or (
                process.process and process.process.returncode is not None
            )

            if url_changed or process_failed:
                if url_changed:
                    logger.info(
                        f"Stream {stream_key} URL changed (failover), restarting with new URL: {url}"
                    )
                else:
                    logger.warning(
                        f"Existing process for stream {stream_key} is unhealthy, recreating..."
                    )
                await self._cleanup_local_process(stream_key)
                # Process will be recreated below with new URL
            else:
                # Process is healthy and URL unchanged, reuse it
                await process.add_client(client_id)
                self.client_streams[client_id] = stream_key
                return stream_key, process

        # If sharing enabled, check Redis for existing streams
        if self.enable_sharing and self.redis_client:
            redis_key = f"stream:{stream_key}"
            stream_data = await self.redis_client.hgetall(redis_key) or {}

            if stream_data and await self._is_redis_stream_healthy(stream_data):
                owner = stream_data.get("owner")
                if owner != self.worker_id:
                    logger.info(
                        f"Stream {stream_key} is managed by another worker ({owner}). This worker will not create a local copy."
                    )
                    raise ConnectionAbortedError(f"Stream is on another worker {owner}")

        # Create new local process
        process = SharedTranscodingProcess(
            stream_key,
            url,
            profile,
            ffmpeg_args,
            user_agent=user_agent,
            headers=headers,
            metadata=metadata,
            hls_base_dir=self.hls_base_dir,
        )

        if await process.start_process():
            self.shared_processes[stream_key] = process
            await process.add_client(client_id)
            self.client_streams[client_id] = stream_key

            # Restore stream_id -> stream_key mapping (may have been deleted by _cleanup_local_process)
            if stream_id:
                self.stream_key_to_id[stream_key] = stream_id

            # Register in Redis
            if self.redis_client:
                redis_key = f"stream:{stream_key}"
                stream_data = {
                    "url": url,
                    "profile": profile,
                    "owner": self.worker_id,
                    "created_at": time.time(),
                    "last_access": time.time(),
                    "status": "running",
                    "ffmpeg_pid": process.process.pid if process.process else 0,
                }
                await self.redis_client.hset(redis_key, mapping=stream_data)
                await self.redis_client.sadd(
                    f"worker:{self.worker_id}:streams", redis_key
                )

            logger.info(
                f"Created new shared stream {stream_key} for {len(process.clients)} clients"
            )
            return stream_key, process
        else:
            raise Exception(
                f"Failed to start transcoding process for stream {stream_key}"
            )

    async def _is_redis_stream_healthy(self, stream_data: Dict) -> bool:
        """Check if a Redis stream entry represents a healthy stream"""

        # Check if owner worker is alive
        owner = stream_data.get("owner")
        if not owner or not self.redis_client:
            return False

        worker_data = await self.redis_client.hget("workers", owner)
        if not worker_data:
            return False

        try:
            worker_info = json.loads(worker_data)
            last_seen = worker_info.get("last_seen", 0)
            return time.time() - last_seen < self.heartbeat_interval * 2
        except json.JSONDecodeError:
            return False

    async def remove_client_from_stream(self, client_id: str):
        """Remove a client from its stream"""
        if client_id not in self.client_streams:
            return

        stream_key = self.client_streams.pop(client_id, None)

        if stream_key and stream_key in self.shared_processes:
            process = self.shared_processes[stream_key]
            await process.remove_client(client_id)

            # If no more clients, immediately schedule cleanup
            if not process.clients:
                logger.info(
                    f"No more clients for stream {stream_key}, scheduling immediate cleanup"
                )
                # Use configured grace period (SHARED_STREAM_GRACE) so short client
                # reconnects (e.g. range-based reconnects from players) don't
                # immediately kill the transcoding process. Fall back to 1s if
                # the setting isn't available.
                grace = int(getattr(settings, "SHARED_STREAM_GRACE", 1))
                asyncio.create_task(
                    self._delayed_cleanup_if_empty(stream_key, grace_period=grace)
                )

    async def force_stop_stream(self, stream_key: str):
        """
        Immediately stop a stream and its FFmpeg process without grace period.
        Used when explicitly deleting a stream via API.
        """
        if stream_key not in self.shared_processes:
            logger.info(f"Stream {stream_key} not found in local processes")
            return False

        logger.info(
            f"Force stopping stream {stream_key} and terminating FFmpeg process"
        )
        process = self.shared_processes[stream_key]

        # Remove all clients from this stream immediately
        clients_to_remove = list(process.clients.keys())
        for client_id in clients_to_remove:
            await process.remove_client(client_id)
            if client_id in self.client_streams:
                del self.client_streams[client_id]

        # Immediately cleanup the FFmpeg process
        await self._cleanup_local_process(stream_key)

        logger.info(f"Stream {stream_key} force stopped, FFmpeg process terminated")
        return True

    async def _delayed_cleanup_if_empty(self, stream_key: str, grace_period: int = 10):
        """
        Clean up a stream after a grace period if it still has no clients.
        This prevents immediate termination on brief disconnects while ensuring
        resources are freed quickly when streaming actually stops.
        """
        await asyncio.sleep(grace_period)

        if stream_key not in self.shared_processes:
            return  # Already cleaned up

        process = self.shared_processes[stream_key]

        # Check if clients reconnected during grace period
        if not process.clients:
            logger.info(
                f"Grace period expired for stream {stream_key} with no clients, cleaning up FFmpeg process"
            )
            await self._cleanup_local_process(stream_key)
        else:
            logger.info(
                f"Clients reconnected to stream {stream_key} during grace period, keeping process alive"
            )

    def update_client_activity(self, client_id: str):
        """Update the last access time for a client."""
        if client_id in self.client_streams:
            stream_key = self.client_streams[client_id]
            if stream_key in self.shared_processes:
                process = self.shared_processes[stream_key]
                if client_id in process.clients:
                    process.clients[client_id] = time.time()

    async def stream_shared_process(
        self, client_id: str
    ) -> Optional[asyncio.subprocess.Process]:
        """Get the FFmpeg process for a client's stream"""

        if client_id not in self.client_streams:
            return None

        stream_key = self.client_streams[client_id]
        if stream_key not in self.shared_processes:
            return None

        process = self.shared_processes[stream_key]
        return process.process

    async def get_stream_stats(self) -> Dict[str, Any]:
        """Get statistics about active streams"""

        stats = {
            "worker_id": self.worker_id,
            "sharing_enabled": self.enable_sharing,
            "local_streams": len(self.shared_processes),
            "total_clients": len(self.client_streams),
            "streams": [],
        }

        for stream_id, process in self.shared_processes.items():
            stream_stats = {
                "stream_id": stream_id,
                "url": process.url,
                "profile": process.profile,
                "client_count": len(process.clients),
                "created_at": process.created_at,
                "last_access": process.last_access,
                "status": process.status,
                "total_bytes_served": process.total_bytes_served,
            }
            stats["streams"].append(stream_stats)

        return stats
