import requests, csv

base_url = 'https://api.census.gov/data/2019/acs/acs5'

# Define the parameters for the API request
params = {
    'get': 'NAME,B01003_001E',  
    'for': 'state:*'
}


response = requests.get(base_url, params=params)

if response.status_code == 200:
    # print(response.json())
    data = response.json()
    #  Specify the path to the CSV file
    csv_file_path = 'data.csv'

    # Open the CSV file for writing
    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        print("isinstance(data, list): ",isinstance(data, list), " \nlen(data: ", len(data), " \nisinstance(data[0], dict): ",isinstance(data[0], dict))

        # Check if the data is a list of dictionaries (common format)
        if isinstance(data, list) and len(data) > 0 :
            # Write the header (keys of the first dictionary)
            header = data[0]
            writer.writerow(header)
            data.pop(0)
            # Write the rows (values of each dictionary)
            for item in data:
                writer.writerow(item)
        else:
            # Handle other cases or raise an error
            print("Unexpected data format")

    print(f"Data has been written to {csv_file_path}")
    # Explanation:
else:
    print(f"Error: {response.status_code} - {response.text}")


