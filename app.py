import requests
import xml.etree.ElementTree as ET
import csv
import os
from datetime import datetime, timedelta
from atproto import Client
# This import is essential for creating the rich text facets (clickable links)
from atproto.xrpc_client.models import main as Main

def crawl_and_post():
    """
    Crawls the mass.gov sitemap index, finds URLs modified yesterday,
    saves them to a dated CSV file, and posts a summary to Bluesky.
    """
    # --- Configuration ---
    SITEMAP_INDEX_URL = "https://www.mass.gov/sitemap.xml"
    YESTERDAY = datetime.now().date() - timedelta(days=1)
    OUTPUT_DIR = "daily_updates"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    OUTPUT_FILE = os.path.join(OUTPUT_DIR, f"{YESTERDAY.strftime('%Y-%m-%d')}.csv")
    
    # --- Bluesky Configuration ---
    BLUESKY_HANDLE = os.environ.get("BLUESKY_HANDLE")
    BLUESKY_APP_PASSWORD = os.environ.get("BLUESKY_APP_PASSWORD")
    REPO_URL = os.environ.get("GITHUB_REPOSITORY") 

    if not all([BLUESKY_HANDLE, BLUESKY_APP_PASSWORD, REPO_URL]):
        print("❌ Missing one or more required environment variables for Bluesky.")
        
    print(f"Starting crawl of {SITEMAP_INDEX_URL}...")
    print(f"Looking for pages modified on {YESTERDAY.strftime('%Y-%m-%d')}")

    ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    found_urls = []

    try:
        index_response = requests.get(SITEMAP_INDEX_URL)
        index_response.raise_for_status()
        index_root = ET.fromstring(index_response.content)
        sitemap_urls = [elem.text for elem in index_root.findall('sitemap:sitemap/sitemap:loc', ns)]
        print(f"Found {len(sitemap_urls)} individual sitemaps to crawl.")

        for sitemap_url in sitemap_urls:
            try:
                sitemap_response = requests.get(sitemap_url)
                sitemap_response.raise_for_status()
                sitemap_root = ET.fromstring(sitemap_response.content)
                for url_entry in sitemap_root.findall('sitemap:url', ns):
                    loc_element = url_entry.find('sitemap:loc', ns)
                    lastmod_element = url_entry.find('sitemap:lastmod', ns)
                    if loc_element is not None and lastmod_element is not None:
                        url = loc_element.text
                        lastmod_text = lastmod_element.text
                        mod_date = datetime.fromisoformat(lastmod_text).date()
                        if mod_date == YESTERDAY:
                            found_urls.append([url, lastmod_text])
                            print(f"  ✅ Match found: {url}")
            except requests.exceptions.RequestException as e:
                print(f"  ❌ Could not process sitemap {sitemap_url}: {e}")
            except ET.ParseError as e:
                print(f"  ❌ Error parsing XML for {sitemap_url}: {e}")
    except requests.exceptions.RequestException as e:
        print(f"FATAL: Could not fetch the main sitemap index: {e}")
        return

    if not found_urls:
        print("-" * 20)
        print("Crawl complete. No pages found updated yesterday.")
        return

    print("-" * 20)
    print(f"Crawl complete. Found {len(found_urls)} matching entries.")
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['URL', 'Last Modified'])
        csv_writer.writerows(found_urls)
    print(f"Results saved to '{OUTPUT_FILE}'.")

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
        results_link_in_repo = f"https://github.com/{REPO_URL}/blob/main/{OUTPUT_FILE}"

        # --- Create Rich Text with clickable links (facets) ---
        post_text = f"{first_url}"
        if num_others > 0:
            post_text += f" and {num_others} others were"
        else:
            post_text += " was"
        post_text += f" updated on {update_date_str}. See all updates: {results_link_in_repo}"

        # Truncate if necessary to stay under the 300 character limit
        if len(post_text.encode('utf-8')) > 300:
            # A simple way to truncate the first URL to make space
            overhead = len(post_text) - len(first_url)
            allowed_url_len = 300 - overhead - 3 # -3 for "..."
            first_url_display = f"{first_url[:allowed_url_len]}..."
            post_text = f"{first_url_display}" + post_text[len(first_url):]
        else:
            first_url_display = first_url

        facets = []
        # Create a facet for the first URL
        # The feature is the full URL, the index is based on the (potentially truncated) display text
        facets.append(Main.Facet(
            index=Main.ByteSlice(byteStart=0, byteEnd=len(first_url_display.encode('utf-8'))),
            features=[Main.Link(uri=first_url)]
        ))
        
        # Create a facet for the GitHub repository link
        repo_link_start_index = post_text.find(results_link_in_repo)
        start_bytes = len(post_text[:repo_link_start_index].encode('utf-8'))
        end_bytes = start_bytes + len(results_link_in_repo.encode('utf-8'))
        facets.append(Main.Facet(
            index=Main.ByteSlice(byteStart=start_bytes, byteEnd=end_bytes),
            features=[Main.Link(uri=results_link_in_repo)]
        ))

        # Send the post with the text and the facets that make links clickable
        client.send_post(text=post_text, facets=facets)
        print("✅ Successfully posted to Bluesky with clickable links.")

    except Exception as e:
        print(f"❌ Failed to post to Bluesky: {e}")

if __name__ == "__main__":
    crawl_and_post()
