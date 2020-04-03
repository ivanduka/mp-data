from io import StringIO
from contextlib import redirect_stdout, redirect_stderr
import re
from bs4 import BeautifulSoup
import traceback

whitespace = r'\s+'  # all white space


def table_checker(args):
    doc_text, doc_text_rotated, doc_id, toc_id, toc_page, s1_rex, s2_rex = args
    buf = StringIO()
    page_list = []
    with redirect_stdout(buf), redirect_stderr(buf):
        try:
            print("Start.")
            # check unrotated
            soup = BeautifulSoup(doc_text, 'lxml')
            pages = soup.find_all('div', attrs={'class': 'page'})
            for page_num, page in enumerate(pages):
                text_clean = re.sub(whitespace, ' ', page.text)
                # text_clean = re.sub(punctuation, ' ', text_clean)
                if re.search(s1_rex, text_clean) and re.search(s2_rex, text_clean):
                    if (doc_id != toc_id) or (page_num != toc_page):
                        page_list.append(page_num)

            # check rotated
            soup = BeautifulSoup(doc_text_rotated, 'lxml')
            pages = soup.find_all('div', attrs={'class': 'page'})
            for page_num, page in enumerate(pages):
                text_clean = re.sub(whitespace, ' ', page.text)
                # text_clean = re.sub(punctuation, ' ', text_clean)
                if re.search(s1_rex, text_clean) and re.search(s2_rex, text_clean):
                    if (doc_id != toc_id) or (page_num != toc_page):
                        page_list.append(page_num)
            print(f"Success. Found data on {len(page_list)} pages.")
            return True, buf.getvalue(), page_list, doc_id
        except Exception as e:
            traceback.print_tb(e.__traceback__)
            return False, buf.getvalue(), page_list, doc_id
