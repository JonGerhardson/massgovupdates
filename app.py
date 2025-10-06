import requests
import xml.etree.ElementTree as ET
import csv
import os
from datetime import datetime, timedelta

import os
from datetime import datetime, timedelta
aafrom atproto import Client

def crawl_and_post():
    """
    Crawls the mass.gov sitemap index, finds URLs modified yesterday,
    saves them to a dated CSV file, and posts a summary to Bluesky.
    """
    # --- Configuration ---
    SITEMAP_INDEX_URL = "https://www.mass.gov/sitemap.xml"
    # Dynamically set the target date to yesterday
    YESTERDAY = datetime.now().date() - timedelta(days=1)
    # Create a directory for daily updates if it doesn't exist
    OUTPUT_DIR = "daily_updates"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # Set the output file name based on yesterday's date
    OUTPUT_FILE = os.path.join(OUTPUT_DIR, f"{YESTERDAY.strftime('%Y-%m-%d')}.csv")
    
    # --- Bluesky Configuration ---
    # Credentials will be read from environment variables in GitHub Actions
    BLUESKY_HANDLE = os.environ.get("BLUESKY_HANDLE")
    BLUESKY_APP_PASSWORD = os.environ.get("BLUESKY_APP_PASSWORD")
    # Your GitHub repository URL (e.g., "your_username/your_repo_name")
    REPO_URL = os.environ.get("GITHUB_REPOSITORY") 

    if not all([BLUESKY_HANDLE, BLUESKY_APP_PASSWORD, REPO_URL]):
        print("❌ Missing one or more required environment variables for Bluesky.")
        print("   Please set BLUESKY_HANDLE, BLUESKY_APP_PASSWORD, and GITHUB_REPOSITORY.")
        # We can still run the crawl, just can't post.
        # return 
        
    print(f"Starting crawl of {SITEMAP_INDEX_URL}...")
    print(f"Looking for pages modified on {YESTERDAY.strftime('%Y-%m-%d')}")

    ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    found_urls = []

    try:
        # 1. Fetch the main sitemap index
        index_response = requests.get(SITEMAP_INDEX_URL)
        index_response.raise_for_status()

        # 2. Parse the index XML
        index_root = ET.fromstring(index_response.content)
        sitemap_urls = [
            elem.text for elem in index_root.findall('sitemap:sitemap/sitemap:loc', ns)
        ]
        print(f"Found {len(sitemap_urls)} individual sitemaps to crawl.")

        # 3. Loop through each individual sitemap
        for sitemap_url in sitemap_urls:
            try:
                sitemap_response = requests.get(sitemap_url)
                sitemap_response.raise_for_status()
                sitemap_root = ET.fromstring(sitemap_response.content)

                # 4. Find all <url> entries
                for url_entry in sitemap_root.findall('sitemap:url', ns):
                    loc_element = url_entry.find('sitemap:loc', ns)
                    lastmod_element = url_entry.find('sitemap:lastmod', ns)

                    if loc_element is not None and lastmod_element is not None:
                        url = loc_element.text
                        lastmod_text = lastmod_element.text
                        mod_date = datetime.fromisoformat(lastmod_text).date()

                        # 5. Check if the modification date is yesterday
                        if mod_date == YESTERDAY:
                            found_urls.append([url, lastmod_text])
                            print(f"  ✅ Match found: {url}")

            except requests.exceptions.RequestException as e:
                print(f"  ❌ Could not fetch or process sitemap {sitemap_url}: {e}")
            except ET.ParseError as e:
                print(f"  ❌ Error parsing XML for {sitemap_url}: {e}")

    except requests.exceptions.RequestException as e:
        print(f"FATAL: Could not fetch the main sitemap index: {e}")
        return

    # --- Save results to CSV ---
    if found_urls:
        print("-" * 20)
        print(f"Crawl complete. Found {len(found_urls)} matching entries.")
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['URL', 'Last Modified'])
            csv_writer.writerows(found_urls)
        print(f"Results saved to '{OUTPUT_FILE}'.")
    else:
        print("-" * 20)
        print("Crawl complete. No pages found updated yesterday.")
        return # Exit if no updates, no need to post

    # --- Post to Bluesky ---
    if not all([BLUESKY_HANDLE, BLUESKY_APP_PASSWORD, REPO_URL]):
        print("\nSkipping Bluesky post due to missing credentials.")
        return

    try:
        print("\nAttempting to post to Bluesky...")
        client = Client()
        client.login(BLUESKY_HANDLE, BLUESKY_APP_PASSWORD)

        first_url = found_urls[0][0]
        num_others = len(found_urls) - 1
        update_date_str = YESTERDAY.strftime('%B %d, %Y')
        
        # Construct the link to the results file in the GitHub repo
        results_link_in_repo = f"https://github.com/{REPO_URL}/blob/main/{OUTPUT_FILE}"

        # Construct the post text
        post_text = f"{first_url}"
        if num_others > 0:
            post_text += f" and {num_others} others were"
        else:
            post_text += " was"
        post_text += f" updated on {update_date_str}. See all updates: {results_link_in_repo}"

        # Truncate if necessary (Bluesky limit is 300 chars)
        if len(post_text) > 300:
            # Simple truncation, leaving space for "..."
            truncate_at = 300 - len(f" and {num_others} others...") - len(results_link_in_repo) - 30
            post_text = f"{first_url[:truncate_at]}... and {num_others} others were updated on {update_date_str}. See all updates: {results_link_in_repo}"


        client.send_post(text=post_text)
        print("✅ Successfully posted to Bluesky.")

    except Exception as e:
        print(f"❌ Failed to post to Bluesky: {e}")


if __name__ == "__main__":
    crawl_and_post()
