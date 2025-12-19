import logging
import os
import re
import tempfile
import time
from datetime import datetime
import backoff
from ftplib import FTP_TLS, all_errors
import ssl
import pytz
import singer
from ftplib import error_perm, error_temp, error_proto, error_reply

from tap_ftps import decrypt

LOGGER = singer.get_logger()

logging.getLogger("ftplib").setLevel(logging.CRITICAL)

def handle_backoff(details):
    LOGGER.warn(
        "FTPS Connection closed unexpectedly. Waiting {wait} seconds and retrying...".format(**details)
    )


class FTPSConnection():
    def __init__(self, host, username, password=None, private_key_file=None, port=None, ssl_context=None, timeout=None, connect_timeout=None):
        self.host = host
        self.username = username
        self.password = password
        self.port = int(port or 21)
        self.retries = 10
        self.timeout = timeout or 300  # Default timeout: 5 minutes for data transfers
        self.connect_timeout = connect_timeout or 30  # Default connection timeout: 30 seconds
        self.__ftp = None
        self.current_dir = None  # Remember current working directory for reconnection
        self.ssl_context = ssl_context or self._create_ssl_context()

    def _create_ssl_context(self):
        """Create SSL context with certificate verification disabled for FTPS client"""
        # Use SERVER_AUTH for client connections (to authenticate the server)
        # CLIENT_AUTH is for servers authenticating clients
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.options |= ssl.OP_NO_SSLv3
        # Must disable check_hostname before setting verify_mode to CERT_NONE
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    # If connection is snapped during connect flow, retry up to a
    # minute for FTPS connection to succeed. 2^6 + 2^5 + ...
    @backoff.on_exception(
        backoff.expo,
        (EOFError, ConnectionResetError),
        max_tries=6,
        on_backoff=handle_backoff,
        jitter=None,
        factor=2)
    def __connect(self):
        for i in range(self.retries+1):
            try:
                LOGGER.info(f'Creating new connection to FTPS at {self.host}:{self.port}...')
                
                # Create FTP_TLS connection with SSL context
                # FTP_TLS doesn't accept port in __init__, so we create instance then connect
                self.__ftp = FTP_TLS(context=self.ssl_context)
                # Set connection timeout (shorter for initial connection)
                self.__ftp.timeout = self.connect_timeout
                LOGGER.debug(f'FTP_TLS instance created with connect_timeout={self.connect_timeout}s, attempting to connect...')
                self.__ftp.connect(host=self.host, port=self.port)
                # After successful connection, set longer timeout for data transfers
                self.__ftp.timeout = self.timeout
                LOGGER.debug(f'Connection established, timeout set to {self.timeout}s for data transfers')
                LOGGER.debug('Connection established, attempting login...')
                LOGGER.debug(f'Login attempt - username: {repr(self.username)}, password length: {len(self.password) if self.password else 0}')
                self.__ftp.login(user=self.username, passwd=self.password)
                LOGGER.debug('Login successful, enabling secure data channel...')
                # Enable secure data channel (PBSZ and PROT P)
                self.__ftp.prot_p()
                LOGGER.debug('Secure data channel enabled')
                
                # Restore working directory if we had one
                if self.current_dir:
                    try:
                        self.__ftp.cwd(self.current_dir)
                    except Exception:
                        LOGGER.warning(f"Could not restore directory {self.current_dir}, using current directory")
                
                LOGGER.info('Connection successful')
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
                LOGGER.warning(f'Connection failed (attempt {i+1}/{self.retries+1}): {error_type}: {error_msg}')
                LOGGER.info(f'Connection failed, sleeping for {5*i} seconds...')
                time.sleep(5*i)
                LOGGER.info("Retrying to establish a connection...")
                if i >= (self.retries):
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
                LOGGER.error(f'Unexpected error during connection (attempt {i+1}/{self.retries+1}): {error_type}: {error_msg}')
                LOGGER.debug(f'Traceback: {traceback.format_exc()}')
                LOGGER.info(f'Connection failed, sleeping for {5*i} seconds...')
                time.sleep(5*i)
                LOGGER.info("Retrying to establish a connection...")
                if i >= (self.retries):
                    raise ex

    @property
    def ftp(self):
        if self.__ftp is None:
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

        try:
            # Use MLSD for detailed file information (similar to listdir_attr in SFTP)
            # MLSD returns list of (name, facts_dict) tuples
            result = list(self.ftp.mlsd(prefix))
        except (error_perm, error_temp, error_proto, error_reply) as e:
            # If MLSD is not supported, fall back to LIST command
            LOGGER.debug(f"MLSD not supported, falling back to LIST for {prefix}")
            try:
                result = self._list_with_details(prefix)
            except Exception as list_error:
                # Re-raise the original MLSD error, not the LIST error
                if isinstance(e, error_perm):
                    raise Exception(f"Permission denied listing directory {prefix}: {e}") from e
                elif isinstance(e, error_temp):
                    raise Exception(f"Temporary error listing directory {prefix}: {e}") from e
                elif isinstance(e, error_proto):
                    raise Exception(f"Protocol error listing directory {prefix}: {e}") from e
                else:
                    raise Exception(f"Error listing directory {prefix}: {e}") from e
        except (OSError, EOFError, ConnectionResetError) as e:
            LOGGER.info("Socket closed. Retrying")
            self.__connect()
            try:
                result = list(self.ftp.mlsd(prefix))
            except:
                result = self._list_with_details(prefix)

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
        self.ftp.retrlines(f'LIST {path}', lines.append)
        
        result = []
        for line in lines:
            # Parse LIST output (format varies by server, but typically: permissions links owner group size date time name)
            parts = line.split(None, 8)
            if len(parts) >= 9:
                name = parts[8]
                # Try to determine if it's a directory (starts with 'd')
                is_dir = parts[0].startswith('d') if parts[0] else False
                # Try to get size
                try:
                    size = int(parts[4]) if len(parts) > 4 else 0
                except (ValueError, IndexError):
                    size = 0
                
                facts = {
                    'type': 'dir' if is_dir else 'file',
                    'size': str(size)
                }
                result.append((name, facts))
        return result

    def get_files(self, prefix, search_pattern, modified_since=None, search_subdirectories=True):
        files = self.get_files_by_prefix(prefix, search_subdirectories)
        if files:
            LOGGER.info('Found %s files in "%s"', len(files), prefix)
            # Log sample filenames for debugging
            sample_filenames = [os.path.basename(f['filepath']) for f in files[:5]]
            LOGGER.debug(f"Sample filenames: {sample_filenames}")
        else:
            LOGGER.warning('Found no files on specified FTPS server at "%s"', prefix)

        matching_files = self.get_files_matching_pattern(files, search_pattern)

        if matching_files:
            LOGGER.info('Found %s files in "%s" matching "%s"', len(matching_files), prefix, search_pattern)
        else:
            LOGGER.warning('Found no files on specified FTPS server at "%s" matching "%s"', prefix, search_pattern)

        for f in matching_files:
            LOGGER.info("Found file: %s", f['filepath'])

        if modified_since is not None:
            matching_files = [f for f in matching_files if f["last_modified"] > modified_since]

        return matching_files

    def get_file_handle(self, f, decryption_configs=None):
        """ Takes a file dict {"filepath": "...", "last_modified": "..."} and returns a handle to the file. """
        with tempfile.TemporaryDirectory() as tmpdirname:
            ftps_file_path = f["filepath"]
            local_path = f'{tmpdirname}/{os.path.basename(ftps_file_path)}'
            if decryption_configs:
                LOGGER.info(f'Decrypting file: {ftps_file_path}')
                # Download FTPS file to local, then decrypt
                try:
                    with open(local_path, 'wb') as local_file:
                        self.ftp.retrbinary(f'RETR {ftps_file_path}', local_file.write)
                except (OSError, EOFError, ConnectionResetError, TimeoutError) as e:
                    LOGGER.warning(f"Connection error during download, retrying: {e}")
                    self.__connect()
                    with open(local_path, 'wb') as local_file:
                        self.ftp.retrbinary(f'RETR {ftps_file_path}', local_file.write)
                
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
                    raise Exception(f'Decryption of file failed: {ftps_file_path}')
            else:
                try:
                    with open(local_path, 'wb') as local_file:
                        self.ftp.retrbinary(f'RETR {ftps_file_path}', local_file.write)
                except (OSError, EOFError, ConnectionResetError, TimeoutError) as e:
                    LOGGER.warning(f"Connection error during download, retrying: {e}")
                    self.__connect()
                    with open(local_path, 'wb') as local_file:
                        self.ftp.retrbinary(f'RETR {ftps_file_path}', local_file.write)
                return open(local_path, 'rb')

    def get_files_matching_pattern(self, files, pattern):
        """ Takes a file dict {"filepath": "...", "last_modified": "..."} and a regex pattern string, and returns
            files matching that pattern. Matches against filename only (basename), not full path. """
        matcher = re.compile(pattern)
        LOGGER.info(f"Searching for files for matching pattern: {pattern}")
        
        # Debug: Log sample filenames to help troubleshoot
        if files:
            sample_filenames = [os.path.basename(f["filepath"]) for f in files[:5]]
            LOGGER.debug(f"Sample filenames found: {sample_filenames}")
        
        # Match against filename only (basename), not full path
        # This is more intuitive and matches user expectations
        matching = []
        for f in files:
            filename = os.path.basename(f["filepath"])
            if matcher.search(filename):
                matching.append(f)
                LOGGER.debug(f"Match found: {filename} (full path: {f['filepath']})")
        
        if not matching and files:
            LOGGER.warning(
                f"No files matched pattern '{pattern}'. "
                f"Pattern is matched against filename only (not full path)."
            )
        
        return matching


def connection(config):
    return FTPSConnection(config['host'],
                          config['username'],
                          password=config.get('password'),
                          port=config.get('port'),
                          timeout=config.get('timeout'),
                          connect_timeout=config.get('connect_timeout'))