#!/usr/bin/env python3
import asyncio
from browserbase_enricher import BrowserbaseEnricher
import json
import os
import sys

async def main():
    """Test the Browserbase enricher with some test cases."""
    test_cases = [
        ("LIST Kitchens", "canberra", "act"),
        ("LITE MASTER (ELECTRICAL) PTY LTD", "sydney", "nsw"),
        ("TARNEL CONSTRUCTIONS PTY LTD", "sydney", "nsw"),
        ("MARABANI CONSTRUCTIONS PTY LTD", "sydney", "nsw"),
    ]
    
    # Initialize the enricher (use 'remote' for anti-bot protection, 'local' for testing)
    enricher = BrowserbaseEnricher(mode="local")
    
    for business_name, city, state in test_cases:
        try:
            phone = enricher.search_google_maps(business_name, city, state)
            print(f"{business_name} ({city}, {state}): Phone = {phone if phone else 'Not found'}")
        except Exception as e:
            print(f"Error processing {business_name}: {e}")
    
    enricher.stop()

if __name__ == "__main__":
    # Run the async main function
    import asyncio
    asyncio.run(main())