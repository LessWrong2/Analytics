import pandas as pd
import numpy as np
import html2text


def htmlBody2plaintext(html_series, ignore_links=False):
    h = html2text.HTML2Text()
    h.ignore_links = ignore_links

    return html_series.apply(lambda x: h.handle(x).replace('\n\n','') if type(x)==str else np.nan)
