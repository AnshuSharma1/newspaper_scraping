"""
newspaper3k was not able to process data with downloading punkt model from nltk which was in turn showing ssl
connection error. Have added fix for that
"""

import nltk
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

nltk.download('punkt')
