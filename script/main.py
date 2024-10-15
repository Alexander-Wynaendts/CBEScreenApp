from .search_website_url import search_website_url
from .cbe_formatting import cbe_formatting
from .cbe_screening import cbe_screening
import time
import pandas as pd

def process_in_batches(data, batch_size=25):
    num_batches = len(data) // batch_size + 1

    results = []
    for batch_num in range(num_batches):
        batch_start = batch_num * batch_size
        batch_end = min((batch_num + 1) * batch_size, len(data))
        batch_data = data[batch_start:batch_end]

        if len(batch_data) > 0:
            print(f"Processing batch {batch_num + 1} / {num_batches}")

            # Apply CBE screening to the data
            batch_data = cbe_screening(batch_data)

            # If the batch is not empty, search for website URLs
            if not batch_data.empty:
                batch_data = search_website_url(batch_data)

            results.append(batch_data)

            # Pause between requests to avoid overwhelming the server
            time.sleep(2)  # Adjust the sleep time as necessary

    # Combine all results into a single DataFrame
    final_result = pd.concat(results, ignore_index=True)
    return final_result

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

    # Process the data in batches to prevent overloading
    startup_data = process_in_batches(startup_data, batch_size=25)

    # Print the final number of rows after the entire process
    print(f"Final Screening: {len(startup_data)}")

    # Record the end time and calculate the elapsed time
    end_time = time.time()
    elapsed_time = end_time - start_time

    # Print the total time taken by the process
    print(f"Total process time: {elapsed_time:.2f} seconds")

    return startup_data
