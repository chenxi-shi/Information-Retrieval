import re

import bs4

from resolve_email_files import get_all_info, resolve_features
import settings


class Cleaning_Text(object):
    def __init__(self, eng_words_file):
        self.eng_words_set = set()
        self.stop_words_set = set()
        Cleaning_Text.get_eng_words_set(self, eng_words_file)

    def get_eng_words_set(self, _eng_words_file):
        with open(_eng_words_file, "r", errors="replace", encoding="utf8") as e:
            for _ in e:
                _w = _.strip()
                if _w not in self.stop_words_set:
                    self.eng_words_set.add(_w)

    def remove_un_eng_words(self, _text):
        _text = list(filter(None, re.split(r"[<>,;=@\\/_ \n^&~`]", _text.strip())))
        for _ in _text.copy():
            if _ not in self.eng_words_set:
                try:
                    _text.remove(_)
                except:
                    pass

        _text = " ".join(_text)
        return _text.lower()


if __name__ == "__main__":
    # text, html_only, email_from, email_msg_id, \
    # email_to, email_sbj, email_reply, email_cc, \
    # email_bcc, email_receives, \
    # email_last_receive, email_sent_time = get_all_info("inmail.49946")
    #
    # weird_char, weird_addr, weird_sbj, weird_target, weird_content, \
    # weird_msg_id, servers_count, span_time = resolve_features(text, email_sbj,
    #                                                           email_receives, email_sent_time,
    #                                                           email_last_receive, email_from,
    #                                                           email_msg_id, email_reply,
    #                                                           email_to, email_cc, email_bcc,
    #                                                           settings.greek_alphabet,
    #                                                           settings.wired_char_set)
    _text = '''财务：您好！ 每月有部分增值票、海关缴款书、普通商品销售发 票 本公司以“诚信、快捷、规范”立足于商场中，真诚希望能与贵司合作，也能为贵公司节约一部分资金！ 贵公司生意兴隆、蓬勃发展、业绩蒸蒸日上！！ 联 人：李 手 公司地址：广东深圳市福田区深茂大厦26层'''

    clean_behavior = Cleaning_Text(eng_words_file="English_Words")
    # print(clean_behavior.eng_words_set)
    text = clean_behavior.remove_un_eng_words(_text)
