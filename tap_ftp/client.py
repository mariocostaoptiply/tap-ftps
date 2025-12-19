import logging
import os
import re
import tempfile
import time
from datetime import datetime
import backoff
from ftplib import FTP, FTP_TLS, all_errors
import ssl
import pytz
import singer
from ftplib import error_perm, error_temp, error_proto, error_reply

from tap_ftp import decrypt

LOGGER = singer.get_logger()

logging.getLogger("ftplib").setLevel(logging.CRITICAL)

def handle_backoff(details):
    LOGGER.warn(
        "FTP Connection closed unexpectedly. Waiting {wait} seconds and retrying...".format(**details)
    )


# Global cache for MLSD support per server (host:port)
_mlsd_support_cache = {}

class FTPConnection():
    def __init__(self, host, username, password=None, private_key_file=None, port=None, use_ssl=False, ssl_context=None, timeout=None, connect_timeout=None):
        self.host = host
        self.username = username
        self.password = password
        self.port = int(port or 21)
        self.retries = 10
        self.timeout = timeout or 300  # Default timeout: 5 minutes for data transfers
        self.connect_timeout = connect_timeout or 30  # Default connection timeout: 30 seconds
        self.__ftp = None
        self.current_dir = None  # Remember current working directory for reconnection
        self.use_ssl = use_ssl
        self.ssl_context = ssl_context or (self._create_ssl_context() if use_ssl else None)
        # Use global cache keyed by host:port to persist across connection instances
        self._cache_key = f"{host}:{self.port}"
        self._connection_failed_permanently = False  # Flag to prevent infinite retry loops
        self._total_connection_attempts = 0  # Track total connection attempts across all __connect() calls

    def _create_ssl_context(self):
        """Create SSL context with certificate verification disabled for FTPS client (when use_ssl=True)"""
        # Use SERVER_AUTH for client connections (to authenticate the server)
        # CLIENT_AUTH is for servers authenticating clients
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.options |= ssl.OP_NO_SSLv3
        # Must disable check_hostname before setting verify_mode to CERT_NONE
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    # If connection is snapped during connect flow, retry up to a
    # minute for FTP/FTPS connection to succeed. 2^6 + 2^5 + ...
    @backoff.on_exception(
        backoff.expo,
        (EOFError, ConnectionResetError),
        max_tries=6,
        on_backoff=handle_backoff,
        jitter=None,
        factor=2)
    def __connect(self):
        # Note: MLSD support cache is global and keyed by host:port, so it persists across reconnections
        # Check if we've already exceeded total connection attempts
        if self._total_connection_attempts >= (self.retries + 1):
            self._connection_failed_permanently = True
            raise Exception(f"FTP/FTPS connection failed permanently after {self._total_connection_attempts} total attempts. "
                          f"Cannot reconnect to {self.host}:{self.port}.")
        
        for i in range(self.retries+1):
            # Check total attempts before each retry
            if self._total_connection_attempts >= (self.retries + 1):
                self._connection_failed_permanently = True
                raise Exception(f"FTP/FTPS connection failed permanently after {self._total_connection_attempts} total attempts. "
                              f"Cannot reconnect to {self.host}:{self.port}.")
            
            self._total_connection_attempts += 1
            try:
                protocol = "FTPS" if self.use_ssl else "FTP"
                LOGGER.info(f'Creating new connection to {protocol} at {self.host}:{self.port}... (total attempts: {self._total_connection_attempts})')
                
                # Create FTP or FTP_TLS connection based on use_ssl flag
                if self.use_ssl:
                    # FTP_TLS doesn't accept port in __init__, so we create instance then connect
                    self.__ftp = FTP_TLS(context=self.ssl_context)
                    LOGGER.debug(f'FTP_TLS instance created with connect_timeout={self.connect_timeout}s, attempting to connect...')
                else:
                    # Plain FTP connection
                    self.__ftp = FTP()
                    LOGGER.debug(f'FTP instance created with connect_timeout={self.connect_timeout}s, attempting to connect...')
                
                # Set connection timeout (shorter for initial connection)
                self.__ftp.timeout = self.connect_timeout
                self.__ftp.connect(host=self.host, port=self.port)
                # After successful connection, set longer timeout for data transfers
                self.__ftp.timeout = self.timeout
                LOGGER.debug(f'Connection established, timeout set to {self.timeout}s for data transfers')
                LOGGER.debug('Connection established, attempting login...')
                LOGGER.debug(f'Login attempt - username: {repr(self.username)}, password length: {len(self.password) if self.password else 0}')
                self.__ftp.login(user=self.username, passwd=self.password)
                
                # Enable secure data channel only for FTPS (when use_ssl=True)
                if self.use_ssl:
                    LOGGER.debug('Login successful, enabling secure data channel...')
                    # Enable secure data channel (PBSZ and PROT P)
                    self.__ftp.prot_p()
                    LOGGER.debug('Secure data channel enabled')
                else:
                    LOGGER.debug('Login successful')
                
                # Restore working directory if we had one
                if self.current_dir:
                    try:
                        self.__ftp.cwd(self.current_dir)
                    except Exception:
                        LOGGER.warning(f"Could not restore directory {self.current_dir}, using current directory")
                
                LOGGER.info('Connection successful')
                self._connection_failed_permanently = False  # Reset flag on successful connection
                self._total_connection_attempts = 0  # Reset counter on successful connection
                break
            except (EOFError, ConnectionResetError, TimeoutError) + all_errors as ex:
                if self.__ftp:
                    try:
                        self.__ftp.close()
                    except:
                        pass
                    # Reset to None to ensure clean retry
                    self.__ftp = None
                error_type = type(ex).__name__
                error_msg = str(ex)
                LOGGER.warning(f'Connection failed (attempt {i+1}/{self.retries+1}, total attempts: {self._total_connection_attempts}): {error_type}: {error_msg}')
                LOGGER.info(f'Connection failed, sleeping for 5 seconds...')
                time.sleep(5)
                LOGGER.info("Retrying to establish a connection...")
                if i >= (self.retries):
                    self._connection_failed_permanently = True
                    # Raise specific error for timeout failures
                    if isinstance(ex, TimeoutError):
                        raise Exception(f"FTP/FTPS connection failed permanently due to timeout after {self._total_connection_attempts} total attempts. "
                                      f"Unable to connect to {self.host}:{self.port} within {self.connect_timeout}s timeout. "
                                      f"Last error: {error_type}: {error_msg}") from ex
                    raise ex
            except Exception as ex:
                # Catch any other unexpected exceptions
                if self.__ftp:
                    try:
                        self.__ftp.close()
                    except:
                        pass
                    # Reset to None to ensure clean retry
                    self.__ftp = None
                error_type = type(ex).__name__
                error_msg = str(ex)
                import traceback
                LOGGER.error(f'Unexpected error during connection (attempt {i+1}/{self.retries+1}, total attempts: {self._total_connection_attempts}): {error_type}: {error_msg}')
                LOGGER.debug(f'Traceback: {traceback.format_exc()}')
                LOGGER.info(f'Connection failed, sleeping for 5 seconds...')
                time.sleep(5)
                LOGGER.info("Retrying to establish a connection...")
                if i >= (self.retries):
                    self._connection_failed_permanently = True
                    # Raise specific error for timeout failures
                    if isinstance(ex, TimeoutError):
                        raise Exception(f"FTP/FTPS connection failed permanently due to timeout after {self._total_connection_attempts} total attempts. "
                                      f"Unable to connect to {self.host}:{self.port} within {self.connect_timeout}s timeout. "
                                      f"Last error: {error_type}: {error_msg}") from ex
                    raise ex

    @property
    def ftp(self):
        if self.__ftp is None:
            if self._connection_failed_permanently:
                raise Exception(f"FTP/FTPS connection failed permanently after all retry attempts. Cannot reconnect to {self.host}:{self.port}.")
            self.__connect()
        return self.__ftp

    @ftp.setter
    def ftp(self, ftp):
        self.__ftp = ftp

    def close(self):
        if self.__ftp:
            try:
                self.__ftp.quit()
            except:
                try:
                    self.__ftp.close()
                except:
                    pass
            self.__ftp = None

    def match_files_for_table(self, files, table_name, search_pattern):
        LOGGER.info("Searching for files for table '%s', matching pattern: %s", table_name, search_pattern)
        matcher = re.compile(search_pattern)
        return [f for f in files if matcher.search(f["filepath"])]

    def is_empty(self, facts_dict):
        """Check if file is empty based on MLSD facts"""
        size = facts_dict.get('size', '0')
        try:
            return int(size) == 0
        except (ValueError, TypeError):
            return False

    def is_directory(self, facts_dict):
        """Check if entry is a directory based on MLSD facts"""
        return facts_dict.get('type', '').upper() == 'DIR' or facts_dict.get('type', '').upper() == 'CDIR' or facts_dict.get('type', '').upper() == 'PDIR'

    def _parse_mlsd_date(self, date_str):
        """Parse MLSD date format (YYYYMMDDHHMMSS) to datetime"""
        try:
            if len(date_str) >= 14:
                year = int(date_str[0:4])
                month = int(date_str[4:6])
                day = int(date_str[6:8])
                hour = int(date_str[8:10])
                minute = int(date_str[10:12])
                second = int(date_str[12:14])
                return datetime(year, month, day, hour, minute, second)
        except (ValueError, IndexError):
            pass
        return None

    def get_files_by_prefix(self, prefix, search_subdirectories=True):
        """
        Accesses the underlying file system and gets all files that match "prefix", in this case, a directory path.

        Returns a list of filepaths from the root.
        """
        files = []

        if prefix is None or prefix == '':
            prefix = '.'

        # Check global cache to see if we already know MLSD is not supported for this server
        # If not supported, skip MLSD and go directly to LIST for better performance
        if _mlsd_support_cache.get(self._cache_key) is False:
            # MLSD not supported, use LIST directly
            result = self._list_with_details(prefix)
        else:
            # Try MLSD first (or if we haven't tested yet)
            result = None
            mlsd_supported = True
            
            try:
                # Use MLSD for detailed file information (similar to listdir_attr in SFTP)
                # MLSD returns list of (name, facts_dict) tuples
                result = list(self.ftp.mlsd(prefix))
                # If successful and we haven't cached it yet, mark as supported
                if _mlsd_support_cache.get(self._cache_key) is None:
                    _mlsd_support_cache[self._cache_key] = True
            except (error_perm, error_temp, error_proto, error_reply) as e:
                # Check if it's a "file does not exist" error (550, 450) - directory doesn't exist
                error_msg = str(e).lower()
                error_code = str(e).split()[0] if str(e).split() else ''
                if '550' in error_code or '450' in error_code or 'file does not exist' in error_msg or 'could not list' in error_msg:
                    # Directory doesn't exist - return empty list
                    LOGGER.warning(f"Directory '{prefix}' does not exist, returning empty file list")
                    return []
                # Check if it's a "500 Unknown command" error (MLSD not supported)
                elif '500' in error_msg or 'unknown command' in error_msg or 'not implemented' in error_msg:
                    # Cache that MLSD is not supported for this server
                    if _mlsd_support_cache.get(self._cache_key) is None:
                        LOGGER.info(f"MLSD command not supported by server {self._cache_key}, will use LIST for all subsequent operations")
                        _mlsd_support_cache[self._cache_key] = False
                    # Use LIST directly
                    result = self._list_with_details(prefix)
                else:
                    # Other permission/protocol errors - try reconnecting first
                    LOGGER.warning(f"MLSD failed with error: {e}. Attempting to reconnect and retry...")
                    try:
                        if self._connection_failed_permanently:
                            if isinstance(e, TimeoutError):
                                raise Exception(f"FTP/FTPS MLSD operation failed permanently due to timeout. "
                                              f"Unable to list directory '{prefix}' on {self.host}:{self.port} after all retry attempts. "
                                              f"Operation timeout: {self.timeout}s") from e
                            raise Exception(f"FTP/FTPS connection failed permanently after all retry attempts. Cannot reconnect to {self.host}:{self.port}.") from e
                        self.__connect()
                        result = list(self.ftp.mlsd(prefix))
                        if _mlsd_support_cache.get(self._cache_key) is None:
                            _mlsd_support_cache[self._cache_key] = True
                    except Exception as retry_error:
                        # Check if retry also fails with "file does not exist"
                        retry_error_msg = str(retry_error).lower()
                        retry_error_code = str(retry_error).split()[0] if str(retry_error).split() else ''
                        if '550' in retry_error_code or '450' in retry_error_code or 'file does not exist' in retry_error_msg or 'could not list' in retry_error_msg:
                            LOGGER.warning(f"Directory '{prefix}' does not exist, returning empty file list")
                            return []
                        # If retry also fails with unknown command, cache it
                        elif '500' in retry_error_msg or 'unknown command' in retry_error_msg or 'not implemented' in retry_error_msg:
                            if _mlsd_support_cache.get(self._cache_key) is None:
                                LOGGER.info(f"MLSD command not supported by server {self._cache_key}, will use LIST for all subsequent operations")
                                _mlsd_support_cache[self._cache_key] = False
                        result = self._list_with_details(prefix)
            except (OSError, EOFError, ConnectionResetError, TimeoutError) as e:
                LOGGER.info(f"Socket/timeout error ({type(e).__name__}). Retrying")
                if self._connection_failed_permanently:
                    if isinstance(e, TimeoutError):
                        raise Exception(f"FTP/FTPS operation failed permanently due to timeout. "
                                      f"Connection to {self.host}:{self.port} failed after all retry attempts. "
                                      f"Operation timeout: {self.timeout}s") from e
                    raise Exception(f"FTP/FTPS connection failed permanently after all retry attempts. Cannot reconnect to {self.host}:{self.port}.") from e
                self.__connect()
                try:
                    result = list(self.ftp.mlsd(prefix))
                    if _mlsd_support_cache.get(self._cache_key) is None:
                        _mlsd_support_cache[self._cache_key] = True
                except (error_perm, error_temp, error_proto, error_reply) as mlsd_error:
                    # Check if it's "file does not exist" error
                    error_msg = str(mlsd_error).lower()
                    error_code = str(mlsd_error).split()[0] if str(mlsd_error).split() else ''
                    if '550' in error_code or '450' in error_code or 'file does not exist' in error_msg or 'could not list' in error_msg:
                        LOGGER.warning(f"Directory '{prefix}' does not exist, returning empty file list")
                        return []
                    # Check if it's "unknown command" error
                    elif '500' in error_msg or 'unknown command' in error_msg or 'not implemented' in error_msg:
                        if _mlsd_support_cache.get(self._cache_key) is None:
                            LOGGER.info(f"MLSD command not supported by server {self._cache_key}, will use LIST for all subsequent operations")
                            _mlsd_support_cache[self._cache_key] = False
                    result = self._list_with_details(prefix)
                except (OSError, EOFError, ConnectionResetError, TimeoutError) as conn_error:
                    # Connection/timeout error persists, try LIST as fallback
                    LOGGER.warning(f"Connection/timeout error persisted, falling back to LIST for {prefix}")
                    result = self._list_with_details(prefix)
                except Exception:
                    # Any other error, try LIST
                    result = self._list_with_details(prefix)

        # Ensure result is not None
        if result is None:
            LOGGER.error(f"Failed to get file list for {prefix} using both MLSD and LIST")
            raise Exception(f"Unable to list directory {prefix}: Both MLSD and LIST commands failed")

        for name, facts in result:
            # Skip . and .. entries
            if name in ['.', '..']:
                continue

            # Build full path
            if prefix == '.':
                full_path = name
            else:
                # Normalize path separators
                prefix_normalized = prefix.rstrip('/')
                full_path = f"{prefix_normalized}/{name}"

            # Check if it's a directory
            if self.is_directory(facts) and search_subdirectories:
                # Recursively get files from subdirectory
                files += self.get_files_by_prefix(full_path, search_subdirectories)
            else:
                # It's a file
                if self.is_empty(facts):
                    continue

                # Get modification time
                modify_str = facts.get('modify')
                if modify_str:
                    last_modified = self._parse_mlsd_date(modify_str)
                else:
                    # Fallback: try to get modification time using MDTM
                    try:
                        mdtm_result = self.ftp.sendcmd(f'MDTM {full_path}')
                        # MDTM returns: 213 YYYYMMDDHHMMSS
                        if mdtm_result.startswith('213'):
                            date_str = mdtm_result.split()[1] if len(mdtm_result.split()) > 1 else None
                            last_modified = self._parse_mlsd_date(date_str) if date_str else None
                        else:
                            last_modified = None
                    except:
                        last_modified = None

                if last_modified is None:
                    LOGGER.warning("Cannot read m_time for file %s, defaulting to current epoch time", full_path)
                    last_modified = datetime.utcnow()

                # Store file info (similar to SFTP format)
                files.append({
                    "filepath": full_path,
                    "last_modified": last_modified.replace(tzinfo=pytz.UTC)
                })

        return files

    def _list_with_details(self, path):
        """Fallback method to get file details using LIST command when MLSD is not available"""
        lines = []
        try:
            self.ftp.retrlines(f'LIST {path}', lines.append)
        except (error_perm, error_temp) as e:
            # Check if it's "file does not exist" error (550, 450)
            error_msg = str(e).lower()
            error_code = str(e).split()[0] if str(e).split() else ''
            if '550' in error_code or '450' in error_code or 'file does not exist' in error_msg or 'could not list' in error_msg:
                LOGGER.warning(f"Directory '{path}' does not exist, returning empty file list")
                return []
            # Re-raise other permission/temp errors
            raise
        except (OSError, EOFError, ConnectionResetError, TimeoutError) as e:
            LOGGER.warning(f"Connection/timeout error during LIST command: {type(e).__name__}, reconnecting...")
            if self._connection_failed_permanently:
                if isinstance(e, TimeoutError):
                    raise Exception(f"FTP/FTPS LIST operation failed permanently due to timeout. "
                                  f"Unable to list directory '{path}' on {self.host}:{self.port} after all retry attempts. "
                                  f"Operation timeout: {self.timeout}s") from e
                raise Exception(f"FTP/FTPS connection failed permanently after all retry attempts. Cannot reconnect to {self.host}:{self.port}.") from e
            self.__connect()
            try:
                self.ftp.retrlines(f'LIST {path}', lines.append)
            except (error_perm, error_temp) as e2:
                # Check if it's "file does not exist" error after reconnect
                error_msg = str(e2).lower()
                error_code = str(e2).split()[0] if str(e2).split() else ''
                if '550' in error_code or '450' in error_code or 'file does not exist' in error_msg or 'could not list' in error_msg:
                    LOGGER.warning(f"Directory '{path}' does not exist, returning empty file list")
                    return []
                # Re-raise other errors
                raise
            except (OSError, EOFError, ConnectionResetError, TimeoutError) as e2:
                # If timeout/connection error persists after reconnect, raise with context
                LOGGER.error(f"Connection/timeout error persisted after reconnect during LIST '{path}': {type(e2).__name__}")
                raise Exception(f"Failed to list directory '{path}' after reconnect: {e2}") from e2
        
        result = []
        for line in lines:
            if not line.strip():
                continue
                
            # Parse LIST output (format varies by server, but typically: permissions links owner group size date time name)
            # Unix format: -rw-r--r-- 1 owner group size date time name
            # Windows format: MM-DD-YY HH:MMAM/PM size name
            parts = line.split(None, 8)
            
            if len(parts) >= 9:
                # Unix-style format
                name = parts[8]
                # Try to determine if it's a directory (starts with 'd')
                is_dir = parts[0].startswith('d') if parts[0] else False
                # Try to get size
                try:
                    size = int(parts[4]) if len(parts) > 4 else 0
                except (ValueError, IndexError):
                    size = 0
            elif len(parts) >= 3:
                # Windows-style format or minimal format
                # Try to extract name (usually last part) and size
                name = parts[-1] if parts else ""
                is_dir = False  # Can't determine from Windows format easily
                try:
                    # Size might be in different positions
                    for part in parts:
                        try:
                            size = int(part)
                            break
                        except ValueError:
                            size = 0
                except (ValueError, IndexError):
                    size = 0
            else:
                # Skip lines that don't match expected format
                LOGGER.debug(f"Skipping unparseable LIST line: {line}")
                continue
            
            # Skip . and .. entries
            if name in ['.', '..']:
                continue
            
            facts = {
                'type': 'dir' if is_dir else 'file',
                'size': str(size)
                # Note: modify date not available from LIST, will use MDTM later
            }
            result.append((name, facts))
        
        if not result:
            LOGGER.warning(f"LIST command returned no parseable entries for {path}")
        
        return result

    def get_files(self, prefix, search_pattern, modified_since=None, search_subdirectories=True):
        files = self.get_files_by_prefix(prefix, search_subdirectories)
        if files:
            LOGGER.info('Found %s files in "%s"', len(files), prefix)
            # Log sample filenames for debugging
            sample_filenames = [os.path.basename(f['filepath']) for f in files[:5]]
            LOGGER.debug(f"Sample filenames: {sample_filenames}")
        else:
            LOGGER.warning('Found no files on specified FTP server at "%s"', prefix)

        matching_files = self.get_files_matching_pattern(files, search_pattern)

        if matching_files:
            LOGGER.info('Found %s files in "%s" matching "%s"', len(matching_files), prefix, search_pattern)
        else:
            LOGGER.warning('Found no files on specified FTP server at "%s" matching "%s"', prefix, search_pattern)

        for f in matching_files:
            LOGGER.info("Found file: %s", f['filepath'])

        if modified_since is not None:
            matching_files = [f for f in matching_files if f["last_modified"] > modified_since]

        return matching_files

    def get_file_handle(self, f, decryption_configs=None):
        """ Takes a file dict {"filepath": "...", "last_modified": "..."} and returns a handle to the file. """
        with tempfile.TemporaryDirectory() as tmpdirname:
            ftp_file_path = f["filepath"]
            local_path = f'{tmpdirname}/{os.path.basename(ftp_file_path)}'
            if decryption_configs:
                LOGGER.info(f'Decrypting file: {ftp_file_path}')
                # Download FTP file to local, then decrypt
                try:
                    with open(local_path, 'wb') as local_file:
                        self.ftp.retrbinary(f'RETR {ftp_file_path}', local_file.write)
                except (OSError, EOFError, ConnectionResetError, TimeoutError) as e:
                    LOGGER.warning(f"Connection error during download, retrying: {e}")
                    if self._connection_failed_permanently:
                        if isinstance(e, TimeoutError):
                            raise Exception(f"FTP/FTPS download failed permanently due to timeout. "
                                          f"Unable to download file '{ftp_file_path}' from {self.host}:{self.port} after all retry attempts. "
                                          f"Operation timeout: {self.timeout}s") from e
                        raise Exception(f"FTP/FTPS connection failed permanently after all retry attempts. Cannot reconnect to {self.host}:{self.port}.") from e
                    self.__connect()
                    with open(local_path, 'wb') as local_file:
                        self.ftp.retrbinary(f'RETR {ftp_file_path}', local_file.write)
                
                decrypted_path = decrypt.gpg_decrypt(
                    local_path,
                    tmpdirname,
                    decryption_configs.get('key'),
                    decryption_configs.get('gnupghome'),
                    decryption_configs.get('passphrase')
                )
                LOGGER.info(f'Decrypting file complete')
                try:
                    return open(decrypted_path, 'rb')
                except FileNotFoundError:
                    raise Exception(f'Decryption of file failed: {ftp_file_path}')
            else:
                try:
                    with open(local_path, 'wb') as local_file:
                        self.ftp.retrbinary(f'RETR {ftp_file_path}', local_file.write)
                except (OSError, EOFError, ConnectionResetError, TimeoutError) as e:
                    LOGGER.warning(f"Connection error during download, retrying: {e}")
                    if self._connection_failed_permanently:
                        if isinstance(e, TimeoutError):
                            raise Exception(f"FTP/FTPS download failed permanently due to timeout. "
                                          f"Unable to download file '{ftp_file_path}' from {self.host}:{self.port} after all retry attempts. "
                                          f"Operation timeout: {self.timeout}s") from e
                        raise Exception(f"FTP/FTPS connection failed permanently after all retry attempts. Cannot reconnect to {self.host}:{self.port}.") from e
                    self.__connect()
                    with open(local_path, 'wb') as local_file:
                        self.ftp.retrbinary(f'RETR {ftp_file_path}', local_file.write)
                return open(local_path, 'rb')

    def get_files_matching_pattern(self, files, pattern):
        """ Takes a file dict {"filepath": "...", "last_modified": "..."} and a regex pattern string, and returns
            files matching that pattern. Matches against filename only (basename), not full path. """
        matcher = re.compile(pattern)
        LOGGER.info(f"Searching for files for matching pattern: {pattern}")
        
        # Always log all filenames found to help troubleshoot (especially in cron jobs)
        if files:
            all_filenames = [os.path.basename(f["filepath"]) for f in files]
            LOGGER.info(f"Found {len(files)} file(s) to check. Filenames: {all_filenames}")
        else:
            LOGGER.warning(f"No files found to match against pattern '{pattern}'")
        
        # Match against filename only (basename), not full path
        # This is more intuitive and matches user expectations
        matching = []
        for f in files:
            filename = os.path.basename(f["filepath"])
            if matcher.search(filename):
                matching.append(f)
                LOGGER.info(f"Match found: {filename} (full path: {f['filepath']})")
        
        if not matching and files:
            LOGGER.warning(
                f"No files matched pattern '{pattern}'. "
                f"Pattern is matched against filename only (not full path). "
                f"Available filenames were: {[os.path.basename(f['filepath']) for f in files]}"
            )
        
        return matching


def connection(config):
    return FTPConnection(config['host'],
                          config['username'],
                          password=config.get('password'),
                          port=config.get('port'),
                          use_ssl=config.get('use_ssl', False),
                          timeout=config.get('timeout'),
                          connect_timeout=config.get('connect_timeout'))