import sys

def bar_progress(current, total,  width=80):

    '''
    Display a progress bar in the console to show the download progress.

    Args:
        - current (int): The current progress value (bytes downloaded).
        - total (int): The total value representing 100% progress (total bytes to download).
        - width (int, optional): The width of the progress bar in characters. Default is 80.

    Returns:
        - None
    '''

    progress_message = "Downloading: %d%% [%d / %d] bytes" % (current / total * 100, current, total)
    sys.stdout.write("\r" + progress_message)
    sys.stdout.flush()

    return
