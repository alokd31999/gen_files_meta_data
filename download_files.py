import os
from bs4 import BeautifulSoup
import requests
import sys
import time
start_time = time.time()

# specify the URL of the archive here
text_file_url="https://github.com/jimmyislive/sample-files"
url_endswith="//github.com"
url_prefix = "https://raw.github.com"
file_endswith = ".txt"
url_string_replace = "/blob"
downloaded_dir_size_threshold_check = 12000000
# Path : https://github.com/jimmyislive/sample-files/blob/master/sample_file_0.txt
# updated raw Path : https://raw.github.com/jimmyislive/sample-files/master/sample_file_0.txt


def get_text_links(text_file_url="https://github.com/jimmyislive/sample-files", url_endswith="//github.com", url_prefix="https://raw.github.com",
                   file_endswith=".txt",  url_string_replace="/blob"):
    # create response object
    r = requests.get(text_file_url)
    # create beautiful-soup object
    soup = BeautifulSoup(r.content, "html.parser")
    # find all links on web-page
    links = soup.findAll('a')
    prefix_url = [link['href'] for link in links if link['href'].endswith(url_endswith)]
    text_links_list1 = [url_prefix + link['href'] for link in links if link['href'].endswith(file_endswith)]
    text_links_list = [i.replace(url_string_replace, "") for i in text_links_list1]
    return text_links_list

def download_urls(url_links, directory="downloaded_input_data_files"):
    """down load file with given url links and store in give directory"""
    print(os.getcwd())
    # Directory
    directory = "downloaded_input_data_files"
    # Parent Directory path
    parent_dir = os.getcwd()
    # Path
    path = os.path.join(parent_dir, directory)
    # Create the directory
    try:
        os.makedirs(path, exist_ok=True)
        print("Directory '%s' created successfully" % directory)
    except OSError as error:
        print("Directory '%s' can not be created" % directory)
        raise OSError
    os.chdir(path)
    print(os.getcwd())
    for link in url_links:
        '''iterate through all links in url_links  
        and download them one by one'''
        # obtain filename by splitting url and getting
        # last string
        file_name = link.split('/')[-1]
        print("Downloading file", file_name)
        # create response object
        r = requests.get(link, stream=True)
        # download started
        with open(file_name, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        print("%s downloaded!\n" % file_name)
    print("All file downloaded!")
    return

def get_directory_size(directory):
    """Returns the `directory` size in bytes."""
    total = 0
    try:
        # print("[+] Getting the size of", directory)
        for entry in os.scandir(directory):
            if entry.is_file():
                # if it's a file, use stat() function
                total += entry.stat().st_size
            elif entry.is_dir():
                # if it's a directory, recursively call this function
                total += get_directory_size(entry.path)
    except NotADirectoryError:
        # if `directory` isn't a directory, get the file size then
        return os.path.getsize(directory)
    except PermissionError:
        # if for whatever reason we can't open the folder, return 0
        return 0
    return total

# Main logic
if __name__ == "__main__":
    try:
        uurls = get_text_links()
        print(uurls)
        download_urls(uurls)
        file_dir = "downloaded_input_data_files"
        # Parent Directory path
        parent_dir1 = os.getcwd()
        if file_dir in parent_dir1:
            file_path_dir = parent_dir1
        else :
            file_path_dir = os.path.join(parent_dir1, file_dir)
    except Exception as e:
        print("Exception is Getting download files Error is -- {}".format(e))
        raise Exception
    finally:
        #dir_size = get_directory_size("C:\\Users\\alok\\PycharmProjects\\pythonProject\\downloaded_input_data_files")
        dir_size = get_directory_size(file_path_dir)
        print("Downloaded files folder size is -- {} in bytes".format(dir_size))
        print("Total time taken download files is - {} minutes".format((time.time() - start_time) / 60))
        if dir_size < downloaded_dir_size_threshold_check:
            print("Exit from job because file size less than -- {} retry or further review ".format(e))
            sys.exit(1)
        else :
            print("Files downloaded successfully and good to go for next step ")

