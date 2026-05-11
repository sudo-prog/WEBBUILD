#!/usr/bin/env python3
"""
Browserbase Enricher — Web scraping with anti-bot protection, CAPTCHA solving, and residential proxies.

Integrates the Browserbase CLI (from https://github.com/browserbase/skills) into the
Supabase Australia lead enrichment pipeline. Use this for Google Maps, Yellow Pages,
and other protected websites that block traditional headless browsers.

Features:
- Anti-bot stealth mode (bypasses Cloudflare, bot detection)
- Automatic CAPTCHA solving (reCAPTCHA, hCaptcha, Turnstile)
- Residential proxies (201 countries, geo-targeting)
- Session persistence (cookies, login state)
- Local mode fallback (no API key required, but less protection)

Setup:
1. Install Node.js and npm
2. Install Browserbase CLI: npm install -g @browserbasehq/browse-cli
3. Get API key from https://browserbase.com/settings
4. Set environment variable: export BROWSERBASE_API_KEY="your_api_key"
5. Ensure Chrome/Chromium is installed (for local mode)

Usage in enrichment scripts:
    from browserbase_enricher import BrowserbaseEnricher
    enricher = BrowserbaseEnricher(mode='remote')  # or 'local' or 'auto'
    phone = enricher.search_google_maps("LIST Kitchens", "canberra", "act")
"""

import subprocess
import json
import time
import os
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

class BrowserbaseEnricher:
    """Browserbase-powered web scraper for lead enrichment."""
    
    def __init__(
        self,
        mode: str = "auto",
        chrome_path: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        mock: bool = False
    ):
        """
        Initialize the enricher.
        
        Args:
            mode: 'auto' (default, chooses remote if API key available else local),
                  'remote' (anti-bot protection), or 'local' (uses local Chrome)
            chrome_path: Path to Chrome/Chromium executable (for local mode)
            api_key: Browserbase API key (overrides env var if provided)
            timeout: Timeout for browser commands in seconds
            max_retries: Maximum number of retries for failed commands
            mock: If True, uses mock data for testing (no browser needed)
        """
        # Set defaults
        if mode == "auto":
            # Auto-detect: use remote if API key available, else local
            self.api_key = api_key or os.environ.get("BROWSERBASE_API_KEY")
            if self.api_key:
                mode = "remote"
            else:
                mode = "local"
                logger.warning(
                    "No BROWSERBASE_API_KEY found. Using local mode (no anti-bot protection)."
                )
        else:
            # Explicit mode, set api_key if provided
            if api_key:
                self.api_key = api_key
            else:
                self.api_key = os.environ.get("BROWSERBASE_API_KEY")
        
        self.mode = mode
        self.chrome_path = chrome_path
        self.timeout = timeout
        self.max_retries = max_retries
        self.session_active = False
        self.mock = mock
        
        # Find the browse CLI executable
        self.browse_cmd = self._find_browse_cli()
        
        # Validate mode
        if self.mode not in ["remote", "local"]:
            raise ValueError("mode must be 'remote' or 'local'")
        
        # Set Chrome path from environment or default
        if not self.chrome_path:
            self.chrome_path = os.environ.get("CHROME_PATH", self._find_chrome())
        
        # Check dependencies
        if not self.mock:
            self._check_dependencies()
    
    def _find_chrome(self) -> Optional[str]:
        """Try to find Chrome/Chromium executable."""
        common_paths = [
            "/usr/bin/chromium",
            "/usr/bin/google-chrome",
            "/usr/bin/chrome",
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/usr/local/bin/chrome"
        ]
        for path in common_paths:
            if Path(path).exists():
                return path
        # Check in PATH
        import shutil
        for cmd in ["chromium", "google-chrome", "chrome"]:
            if shutil.which(cmd):
                return cmd
        return None
    
    def _find_browse_cli(self) -> str:
        """Find the correct browse CLI executable, preferring npm global bin."""
        # Check if browse is in PATH, but verify it's the Browserbase CLI
        import shutil
        browse_path = shutil.which("browse")
        if browse_path:
            # Verify it's the Browserbase CLI by checking version or location
            try:
                result = subprocess.run(
                    [browse_path, "--version"],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode == 0 and "Browserbase" in result.stdout:
                    return browse_path
            except:
                pass
        
        # Fallback to common npm global locations
        npm_global = os.environ.get("NPM_GLOBAL_BIN", "/usr/local/bin")
        possible_paths = [
            "/usr/local/bin/browse",
            "/usr/bin/browse",  # This might be xdg-open, but we check
            "/home/linuxbrew/.linuxbrew/bin/browse",
            "/opt/homebrew/bin/browse"
        ]
        for path in possible_paths:
            if Path(path).exists():
                try:
                    result = subprocess.run(
                        [path, "--version"],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    if result.returncode == 0 and "Browserbase" in result.stdout:
                        return path
                except:
                    pass
        
        # As a last resort, try to find it via npm
        try:
            result = subprocess.run(
                ["npm", "config", "get", "prefix"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                npm_prefix = result.stdout.strip()
                browse_candidate = Path(npm_prefix) / "bin" / "browse"
                if browse_candidate.exists():
                    return str(browse_candidate)
        except:
            pass
        
        # If we can't find it, raise an error
        raise RuntimeError(
            "Browserbase CLI not found. Please install it with:\n"
            "    npm install -g @browserbasehq/browse-cli\n"
            "And make sure the browse command is in your PATH."
        )
    
    def _check_dependencies(self):
        """Check if required dependencies are available."""
        if self.mock:
            logger.info("Mock mode: Skipping dependency checks")
            return
        
        # Check if browse CLI is installed (already found, but double-check)
        try:
            subprocess.run(
                [self.browse_cmd, "--version"],
                capture_output=True,
                check=True
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise RuntimeError(
                "Browserbase CLI not found. Install it with:\n"
                "    npm install -g @browserbasehq/browse-cli\n"
                "Make sure the browse command is in your PATH."
            )
        
        # Check for Chrome in local mode
        if self.mode == "local" and not self.chrome_path:
            raise RuntimeError(
                "Chrome/Chromium not found. Please install Chrome or set CHROME_PATH "
                "to the executable."
            )
        
        # Warn if API key missing for remote mode
        if self.mode == "remote" and not self.api_key:
            logger.warning(
                "No BROWSERBASE_API_KEY set. Remote mode will fail. "
                "Set the environment variable or pass api_key parameter."
            )
    
    def _run_cmd(self, cmd: List[str], check: bool = True) -> subprocess.CompletedProcess:
        """Run a browser command with retries."""
        if self.mock:
            # Return a mock result
            logger.debug(f"Mock running: {' '.join(cmd)}")
            stdout = json.dumps({"mock": True, "command": cmd})
            return subprocess.CompletedProcess(
                args=cmd,
                returncode=0,
                stdout=stdout,
                stderr=""
            )
        
        for attempt in range(1, self.max_retries + 1):
            try:
                # Build command with environment
                full_cmd = [self.browse_cmd] + cmd
                env = os.environ.copy()
                
                # Set Chrome path for local mode
                if self.mode == "local" and self.chrome_path:
                    env["CHROME_PATH"] = self.chrome_path
                
                # Set API key if provided
                if self.api_key:
                    env["BROWSERBASE_API_KEY"] = self.api_key
                
                logger.debug(f"Running: {' '.join(full_cmd)}")
                
                result = subprocess.run(
                    full_cmd,
                    capture_output=True,
                    text=True,
                    timeout=self.timeout,
                    env=env
                )
                
                if check and result.returncode != 0:
                    raise subprocess.CalledProcessError(
                        result.returncode, result.args, result.stderr
                    )
                
                return result
                
            except subprocess.CalledProcessError as e:
                if attempt == self.max_retries:
                    raise
                logger.warning(f"Command failed (attempt {attempt}/{self.max_retries}): {e}")
                time.sleep(2 ** attempt)  # Exponential backoff
            except subprocess.TimeoutExpired:
                if attempt == self.max_retries:
                    raise
                logger.warning(f"Command timed out (attempt {attempt}/{self.max_retries})")
                time.sleep(2 ** attempt)
    
    def _set_environment(self):
        """Set the browser environment (local or remote)."""
        if self.mode == "remote":
            self._run_cmd(["env", "remote"])
        else:
            # Use auto-connect to reuse existing Chrome if possible
            self._run_cmd(["env", "local", "--auto-connect"])
        self.session_active = True
    
    def open(self, url: str) -> Dict[str, Any]:
        """Open a URL in the browser."""
        if not self.session_active:
            self._set_environment()
        result = self._run_cmd(["open", url])
        # Parse JSON if available, otherwise return raw output
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return {"output": result.stdout.strip()}
    
    def snapshot(self) -> Dict[str, Any]:
        """Get an accessibility tree snapshot of the current page."""
        result = self._run_cmd(["snapshot"])
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            # Fallback to parsing key-value pairs
            return self._parse_snapshot(result.stdout)
    
    def get_text(self, selector: str = "body") -> str:
        """Get text content of an element."""
        result = self._run_cmd(["get", "text", selector])
        return result.stdout.strip()
    
    def get_html(self, selector: str = "body") -> str:
        """Get HTML content of an element."""
        result = self._run_cmd(["get", "html", selector])
        return result.stdout.strip()
    
    def click(self, ref: str) -> Dict[str, Any]:
        """Click an element by its reference ID."""
        result = self._run_cmd(["click", ref])
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return {"output": result.stdout.strip()}
    
    def type(self, text: str) -> Dict[str, Any]:
        """Type text into the focused element."""
        result = self._run_cmd(["type", text])
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return {"output": result.stdout.strip()}
    
    def fill(self, selector: str, value: str) -> Dict[str, Any]:
        """Fill an input field and press Enter."""
        result = self._run_cmd(["fill", selector, value])
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return {"output": result.stdout.strip()}
    
    def wait(self, condition: str = "load", timeout: int = 30) -> Dict[str, Any]:
        """Wait for a condition (load, selector, timeout)."""
        result = self._run_cmd(["wait", condition, str(timeout)])
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return {"output": result.stdout.strip()}
    
    def stop(self):
        """Stop the browser session and clean up."""
        if self.session_active:
            try:
                self._run_cmd(["stop"])
            except Exception as e:
                logger.warning(f"Error stopping browser: {e}")
        self.session_active = False
    
    def _parse_snapshot(self, snapshot_text: str) -> Dict[str, Any]:
        """Parse a simplified snapshot format (fallback)."""
        result = {"raw": snapshot_text, "elements": {}}
        lines = snapshot_text.strip().split("\n")
        for line in lines:
            if line.startswith("ELEMENT:") or line.startswith("ELEMENT"):
                parts = line.split("REF:")
                if len(parts) >= 2:
                    after_ref = parts[1].strip()
                    ref_and_desc = after_ref.split(" ", 1)
                    if len(ref_and_desc) == 2:
                        ref, description = ref_and_desc
                        result["elements"][ref.strip()] = description.strip()
        return result
    
    # ── High-level enrichment methods ──────────────────────────────────────────────
    
    def search_google_maps(
        self,
        business_name: str,
        city: str,
        state: str,
        query_suffix: str = "phone number"
    ) -> Optional[str]:
        """Search Google Maps for a business and extract the phone number."""
        if self.mock:
            # Return a mock phone number for testing
            logger.info(f"Mock search_google_maps: {business_name}, {city}, {state}")
            # Return a consistent mock number based on business name hash
            import hashlib
            hash_key = hashlib.md5(business_name.encode()).hexdigest()[:8]
            mock_number = f"0{int(hash_key[:4]) % 450}000000"  # Australian format 0xx xxxxxxx
            return mock_number
        
        # Build query
        query = f"{business_name} {city} {state} {query_suffix}"
        url = f"https://www.google.com/search?q={query}"
        
        try:
            # Open the search results page
            self.open(url)
            self.wait("load", timeout=30)
            
            # Wait a bit for JavaScript to render (especially for maps)
            time.sleep(5)
            
            # Get page content
            content = self.get_text("body")
            
            # Extract phone numbers using regex (Australian format)
            import re
            patterns = [
                r'\(?\d{2,3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # Standard AU phone
                r'\d{2,4}[-.\s]\d{2,4}[-.\s]\d{2,4}',       # Alternative format
                r'\+61\s?[\(]?\d{2,4}[\)]?\s?\d{4,8}'      # International format
            ]
            phones = []
            for pattern in patterns:
                matches = re.findall(pattern, content)
                for match in matches:
                    # Clean the match
                    clean = re.sub(r'[^\d+]', '', match)
                    # Validate: Australian numbers are 10-12 digits (including leading +61)
                    if clean.startswith('61') and len(clean) == 11:
                        # Convert +61 to leading 0 for local format
                        clean = '0' + clean[2:]
                    if len(clean) >= 10 and len(clean) <= 12:
                        phones.append(clean)
            
            # Remove duplicates while preserving order
            seen = set()
            unique_phones = []
            for p in phones:
                if p not in seen:
                    seen.add(p)
                    unique_phones.append(p)
            
            if unique_phones:
                return unique_phones[0]
            else:
                logger.info(f"No phone found for {business_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error in search_google_maps: {e}")
            return None
    
    def search_yellow_pages(
        self,
        business_name: str,
        city: str,
        state: str,
        category: Optional[str] = None
    ) -> Optional[str]:
        """Search Yellow Pages for a business and extract the phone number."""
        # Build Yellow Pages URL
        base_url = "https://www.yellowpages.com.au"
        query = f"{business_name} {city} {state}"
        if category:
            query += f" {category}"
        url = f"{base_url}/search/listings?clue={query}"
        
        try:
            self.open(url)
            self.wait("load", timeout=30)
            time.sleep(3)
            
            # Get page content
            content = self.get_text("body")
            
            # Yellow Pages often has structured data. We can look for phone patterns.
            import re
            patterns = [
                r'\(?\d{2,3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
                r'\d{2,4}[-.\s]\d{2,4}[-.\s]\d{2,4}'
            ]
            phones = []
            for pattern in patterns:
                matches = re.findall(pattern, content)
                for match in matches:
                    clean = re.sub(r'[^\d]', '', match)
                    if len(clean) >= 10:
                        phones.append(clean)
            
            # Remove duplicates
            seen = set()
            unique_phones = []
            for p in phones:
                if p not in seen:
                    seen.add(p)
                    unique_phones.append(p)
            
            if unique_phones:
                return unique_phones[0]
            else:
                logger.info(f"No phone found on Yellow Pages for {business_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error in search_yellow_pages: {e}")
            return None
    
    # Context manager methods for use with 'with' statement
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

# Example usage
if __name__ == "__main__":
    # Test the enricher
    enricher = BrowserbaseEnricher(mode="local")
    try:
        phone = enricher.search_google_maps("LIST Kitchens", "canberra", "act")
        print(f"Phone: {phone}")
    finally:
        enricher.stop()