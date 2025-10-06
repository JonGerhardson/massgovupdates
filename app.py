import requests
import xml.etree.ElementTree as ET
import csv
import os
import logging
from datetime import datetime, timedelta
from atproto import Client
from atproto_client.models import AppBskyRichtextFacet

# --- Basic Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_updated_urls(sitemap_index_url, date_to_check):
    """
    Crawls a sitemap index and finds all URLs modified on a specific date.

    Args:
        sitemap_index_url (str): The URL of the main sitemap index XML.
        date_to_check (datetime.date): The specific date to look for modifications.

    Returns:
        list: A list of lists, where each inner list contains a URL and its
              last modification timestamp. Returns an empty list on failure.
    """
    ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    found_urls = []
    date_str = date_to_check.strftime('%Y-%m-%d')
    logging.info(f"Starting crawl of {sitemap_index_url} for pages modified on {date_str}")

    try:
        index_response = requests.get(sitemap_index_url)
        index_response.raise_for_status()
        index_root = ET.fromstring(index_response.content)
        sitemap_urls = [elem.text for elem in index_root.findall('sitemap:sitemap/sitemap:loc', ns)]
        logging.info(f"Found {len(sitemap_urls)} individual sitemaps to crawl.")

        for sitemap_url in sitemaps_urls:
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
                        # Use slicing to ignore timezone info for broader compatibility
                        mod_date = datetime.fromisoformat(lastmod_text.split('T')[0]).date()
                        if mod_date == date_to_check:
                            found_urls.append([url, lastmod_text])
                            logging.info(f"  ✅ Match found: {url}")
            except requests.exceptions.RequestException as e:
                logging.warning(f"  ❌ Could not process sitemap {sitemap_url}: {e}")
            except ET.ParseError as e:
                logging.warning(f"  ❌ Error parsing XML for {sitemap_url}: {e}")
    except requests.exceptions.RequestException as e:
        logging.critical(f"FATAL: Could not fetch the main sitemap index: {e}")
        return []
    
    return found_urls

def save_results_to_csv(found_urls, output_dir, date_to_check):
    """Saves the list of found URLs to a dated CSV file."""
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{date_to_check.strftime('%Y-%m-%d')}.csv")
    
    logging.info("-" * 20)
    logging.info(f"Crawl complete. Found {len(found_urls)} matching entries.")
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['URL', 'Last Modified'])
        csv_writer.writerows(found_urls)
    logging.info(f"Results saved to '{output_file}'.")
    return output_file

def post_update_to_bluesky(found_urls, csv_filepath, config):
    """Constructs and sends a post to Bluesky with a summary of the findings."""
    try:
        logging.info("\nAttempting to post to Bluesky...")
        client = Client()
        client.login(config["handle"], config["app_password"])

        # --- Prepare content for the post ---
        first_url = found_urls[0][0]
        num_others = len(found_urls) - 1
        update_date_str = (datetime.now().date() - timedelta(days=1)).strftime('%B %d, %Y')
        
        # Construct the link to the results file in the GitHub repo
        csv_filename = os.path.basename(csv_filepath)
        results_link_in_repo = f"https://github.com/{config['repo_owner_and_name']}/blob/main/{config['output_dir']}/{csv_filename}"

        # --- Build and truncate the post text if necessary ---
        base_text = " was" if num_others == 0 else f" and {num_others} others were"
        full_post_template = (
            "{url_display}"
            f"{base_text} updated on {update_date_str}.\n\n"
            f"See all updates: {results_link_in_repo} {config['hashtag']}"
        )
        
        overhead_bytes = len(full_post_template.format(url_display="").encode('utf-8'))
        allowed_url_bytes = 300 - overhead_bytes - 3 # -3 for "..."
        
        first_url_bytes = first_url.encode('utf-8')
        if len(first_url_bytes) > allowed_url_bytes:
            truncated_bytes = first_url_bytes[:allowed_url_bytes]
            while True:
                try:
                    first_url_display = truncated_bytes.decode('utf-8') + "..."
                    break
                except UnicodeDecodeError:
                    truncated_bytes = truncated_bytes[:-1]
        else:
            first_url_display = first_url
        
        post_text = full_post_template.format(url_display=first_url_display)
        
        # --- Create Rich Text Facets (for clickable links) ---
        facets = []
        
        # Facet for the first (potentially truncated) URL
        # NOTE: The class names have changed from Facet, ByteSlice, Link, etc.
        facets.append(AppBskyRichtextFacet.Main(
            index=AppBskyRichtextFacet.ByteSlice(byteStart=0, byteEnd=len(first_url_display.encode('utf-8'))),
            features=[AppBskyRichtextFacet.Link(uri=first_url)]
        ))
        
        # Facet for the GitHub repo link
        repo_link_start_byte = len(post_text.encode('utf-8')) - len(f" {results_link_in_repo} {config['hashtag']}".encode('utf-8')) + 1
        facets.append(AppBskyRichtextFacet.Main(
            index=AppBskyRichtextFacet.ByteSlice(
                byteStart=repo_link_start_byte, 
                byteEnd=repo_link_start_byte + len(results_link_in_repo.encode('utf-8'))
            ),
            features=[AppBskyRichtextFacet.Link(uri=results_link_in_repo)]
        ))
        
        # Facet for the hashtag
        tag_start_byte = len(post_text.encode('utf-8')) - len(config['hashtag'].encode('utf-8'))
        facets.append(AppBskyRichtextFacet.Main(
            index=AppBskyRichtextFacet.ByteSlice(
                byteStart=tag_start_byte,
                byteEnd=tag_start_byte + len(config['hashtag'].encode('utf-8'))
            ),
            features=[AppBskyRichtextFacet.Tag(tag=config['hashtag'].lstrip('#'))] # Tag value does not include '#'
        ))
        
        # --- Send the post ---
        client.send_post(text=post_text, facets=facets)
        logging.info("✅ Successfully posted to Bluesky with rich text.")

    except Exception as e:
        logging.error(f"❌ Failed to post to Bluesky: {e}")


def main():
    """Main function to orchestrate the crawl and post process."""
    # --- Configuration ---
    BLUESKY_HANDLE = os.environ.get("BLUESKY_HANDLE")
    BLUESKY_APP_PASSWORD = os.environ.get("BLUESKY_APP_PASSWORD")
    GITHUB_REPOSITORY = os.environ.get("GITHUB_REPOSITORY")

    config = {
        "sitemap_index_url": "https://www.mass.gov/sitemap.xml",
        "date_to_check": datetime.now().date() - timedelta(days=1),
        "output_dir": "daily_updates",
        "handle": BLUESKY_HANDLE,
        "app_password": BLUESKY_APP_PASSWORD,
        "repo_owner_and_name": GITHUB_REPOSITORY,
        "hashtag": "#mapoli"
    }

    # --- Execution ---
    found_urls = fetch_updated_urls(config["sitemap_index_url"], config["date_to_check"])

    if not found_urls:
        logging.info("Crawl complete. No pages found updated yesterday.")
        return

    csv_filepath = save_results_to_csv(found_urls, config["output_dir"], config["date_to_check"])
    
    if not all([config["handle"], config["app_password"], config["repo_owner_and_name"]]):
        logging.warning("\nSkipping Bluesky post due to missing environment variables.")
        return

    post_update_to_bluesky(found_urls, csv_filepath, config)


if __name__ == "__main__":
    main()
