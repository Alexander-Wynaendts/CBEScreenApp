from .search_website_url import search_website_url
from .cbe_formatting import cbe_formatting
from .cbe_screening import cbe_screening
import time
import pandas as pd

def process_cbe_screening_in_batches(data, batch_size=25):
    num_batches = len(data) // batch_size + 1

    results = []
    for batch_num in range(num_batches):
        batch_start = batch_num * batch_size
        batch_end = min((batch_num + 1) * batch_size, len(data))
        batch_data = data[batch_start:batch_end]

        if len(batch_data) > 0:
            print(f"Processing CBE screening for batch {batch_num + 1} / {num_batches}")
            batch_data = cbe_screening(batch_data)
            results.append(batch_data)

            # Pause between batches if needed
            time.sleep(2)  # Adjust the sleep time as necessary

    # Combine all batches into a single DataFrame
    screened_data = pd.concat(results, ignore_index=True)
    return screened_data

def main(files):

    # Record the start time
    start_time = time.time()

    # Format the input files
    startup_data = cbe_formatting(files)

    # Print the number of filtered rows based on NACE code
    print(f"Nace Code Filter: {len(startup_data)}")

    # Check if startup_data is empty, return it if true
    if startup_data.empty:
        return startup_data

    # Process CBE screening in batches
    startup_data = process_cbe_screening_in_batches(startup_data, batch_size=25)

    # Now that all CBE screening is done, proceed with searching for website URLs
    print("Proceeding to search website URLs...")
    #startup_data = search_website_url(startup_data)

    # Print the final number of rows after the entire process
    print(f"Final Screening: {len(startup_data)}")

    # Record the end time and calculate the elapsed time
    end_time = time.time()
    elapsed_time = end_time - start_time

    # Print the total time taken by the process
    print(f"Total process time: {elapsed_time:.2f} seconds")

    return startup_data
