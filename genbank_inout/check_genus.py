import requests

def check_genus(genus_name):
    # ---------- Requisição inicial para buscar os registros ----------
    cookies = {
        'ASPSESSIONIDAACQAQQQ': 'FMKNJDICKFEMBLBFIAEGOILH',
        '__kewlb': '3523420861.1.2102117344.1112292864',
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://www.indexfungorum.org',
        'Referer': 'https://www.indexfungorum.org/Names/Names.asp',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
    }

    data = {
        'SearchBy': 'Name',
        'SearchTerm': genus_name,
        'submit': 'Search',
    }

    response = requests.post('https://www.indexfungorum.org/Names/Names.asp', cookies=cookies, headers=headers, data=data)

    try:
        if response.status_code == 200:
            text = response.text
            text_lower = text.lower()
            if "no records found" in text_lower or "no records match" in text_lower:
                return False
            # IndexFungorum muda o HTML com frequência. Se nao houver mensagem
            # explicita de "sem registros", consideramos o genero valido para
            # nao perder italicizacao por falso negativo.
            return True
        else:
            print(f"Error accessing IndexFungorum: Status {response.status_code}")
            return False
    except Exception as e:
        print(f"Exception occurred: {e}")
        return False
