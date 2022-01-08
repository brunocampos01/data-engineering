import requests


here = os.path.abspath(os.path.dirname(__file__))
path_config = os.path.join(here + '/..')
sys.path.insert(1, path_config)
import config

__version__ = '0.1'
__author__ = 'Bruno Campos'


def my_token(login_url: str, data: dict) -> str:
    token_response = requests.post(login_url, data).json()
    print(token_response)

    return str(token_response['access_token'])


def generate_header(token: str):
    return {'Accept': 'application/json',
            'Authorization': 'Bearer ' + token}


def get_list_by_id(base_url: str, site: str, guid: str, params: str, headers: dict):
    list_URL = base_url + site + "/_api/web/lists('" + guid + "')" + params
    print(list_URL)

    return requests.get(list_URL, headers=headers)


def conn_sharepoint(base_url: str, site: str, username: str, pw: str):
    url = base_url + site
    opener = basic_auth_opener(url, username, pw)
    print(opener)
    site = SharePointSite(url, opener)
    print(site)

    for sp_list in site.lists:
        print(sp_list.id, sp_list.meta['Title'])


def main():
    """
    Lists in sharepoint need convert
    e.g. guid = %7B96ea6597-0da0-4c98-98f4-06f5b8236763%7D
    replace
        '%7B = {
        %7D = }
    """
    base_url = 'https://' + tenant_name + '.sharepoint.com/'
    login_url = 'https://accounts.accesscontrol.windows.net/1a901d06-ddf0-4c3a-88c2-9d4c569ef679/tokens/OAuth/2'

    name_list = 'Lista de Usuário Ausência'
    guid_usuario_ausencia = '{96ea6597-0da0-4c98-98f4-06f5b8236763}'
    params = '/items?$select=Id,Title,OData__x006b_x74,r9r3,zpkd,OData__x0063_sk6'

    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': resource
    }

    token = my_token(login_url=login_url,
                     data=data)
    header = generate_header(token=token)
    conn_sharepoint(base_url=base_url,
                    site=site_governanca,
                    username=username,
                    pw=password)
    dump_list = get_list_by_id(base_url=base_url,
                               site=site_governanca,
                               guid=guid_usuario_ausencia,
                               params=params,
                               headers=header)

    print(dump_list)


if __name__ == '__main__':
    main()
