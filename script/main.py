from .search_website_url import search_website_url
from .cbe_formatting import cbe_formatting
from .cbe_screening import cbe_screening
import time
import pandas as pd

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

    # Split startup_data into chunks of 50
    chunk_size = 50
    chunks = [startup_data[i:i + chunk_size] for i in range(0, len(startup_data), chunk_size)]

    # Initialize an empty DataFrame to store the results
    all_data = pd.DataFrame()

    # Process each chunk
    for idx, chunk in enumerate(chunks):
        print(f"Processing chunk {idx + 1}/{len(chunks)}")
        startup_data_chunk = cbe_screening(chunk)
        all_data = pd.concat([all_data, startup_data_chunk], ignore_index=True)
    startup_data = all_data

    # Print the number of filtered rows after CBE screening
    print(f"CBE Screen Filter: {len(startup_data)}")

    # Check again if all_data is empty, return it if true
    if startup_data.empty:
        return startup_data

    # Search for website URLs in the data
    startup_data = search_website_url(startup_data)

    print(f"Final Screening: {len(startup_data)}")

    # Record the end time and calculate the elapsed time
    end_time = time.time()
    elapsed_time = end_time - start_time

    # Print the total time taken by the process
    print(f"Total process time: {elapsed_time:.2f} seconds")

    return startup_data
