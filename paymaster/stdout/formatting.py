from pyfiglet import figlet_format

def load_title(title:str, version:str = None, reldate:str=None,
               font:str='smslant'):
    res = figlet_format(title, font=font)
    if reldate and version:
        print(res, 'Mortage Risk Analytics - '
              'Version {} ({})'.format(version, reldate))
    elif version:
        print(res, 'Mortage Risk Analytics - Version {}'.format(version))
    else:
        print(res, 'Mortgage Risk Analaytics')