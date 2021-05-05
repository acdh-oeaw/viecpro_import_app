from copy import deepcopy
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.runtime.http.request_options import RequestOptions
import getpass
import re
import requests
from urllib3 import disable_warnings
from pandas import read_excel


class LoadXlsxSharepoint():

    @property
    def password(self):
        pass

    @password.setter
    def password(self, password):
        self._password = password

    @staticmethod
    def shorten_url(input_url):
        pattern = re.compile(r".*.xlsx")
        res = pattern.match(input_url)
        url = res.group(0)

        return url

    def load(self, url, save_to=None, warnings=False, **kwargs):
        if not warnings:
            # suppresses the urllib3 InsecureRequestWarning
            disable_warnings()

        if not url.endswith(".xlsx"):
            url = self.shorten_url(url)
        ctx_auth = AuthenticationContext(url)
        token = ctx_auth.acquire_token_for_user(self._user, self._password)
        options = RequestOptions(url)
        ctx_auth.authenticate_request(options)
        req = requests.get(url, headers=options.headers, verify=False, allow_redirects=True)

        df = read_excel(req.content, engine="openpyxl", **kwargs)

        if save_to:
            with open(save_to, "wb") as output:
                output.write(req.content)

        return df


    def __init__(self, username=None, password=None):
        if not username:
            username = input('Enter your sharepoint username')
        if not password:
            password = getpass.getpass('Enter your sharepoint password')
        self._user = username
        self._password = password