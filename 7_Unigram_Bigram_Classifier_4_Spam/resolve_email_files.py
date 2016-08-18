'''
mixed
    alternative
        text
        related
            html
            inline image
            inline image
    attachment
    attachment
'''
import email
import re
from os.path import join
from email import utils
from email.parser import Parser

import chardet
import dateutil.parser
import dateparser

import bs4

import settings


def text4html(_string):
    beautifulsoup = bs4.BeautifulSoup
    _soup = beautifulsoup(_string, "html.parser")
    # kill all script and style elements
    for _script in _soup(["script", "style"]):
        _script.extract()  # rip it out
    # get text
    _text = _soup.get_text()

    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in _text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    _text = ' '.join(chunk for chunk in chunks if chunk)
    return _text


# def parse_related(_part_all, _charset):
#     _has_plain_flg = False
#     _text = ""
#     for _part in _part_all.get_payload():  # inside alternative
#         _content_type = _part.get_content_type()
#         if "text" in _content_type:
#             _subtype = _part.get_content_subtype()
#             if "plain" in _subtype:
#                 _has_plain_flg = True
#                 _text += _part.get_payload()
#             elif "html" in _subtype:
#                 if not _has_plain_flg:  # if this html is not the replacement of text
#                     _text += text4html(_part.get_payload())
#             else:
#                 _text += _part.get_payload()
#     return _text, _has_plain_flg


# def parse_alternative(_part_all, _charset):
#     _has_plain_flg = False
#     _text = ""
#     for _part in _part_all.get_payload():  # inside alternative
#         if isinstance(_part, str):
#             return _part, True
#         _content_type = _part.get_content_type()
#         _content_subtype = _part.get_content_subtype()
#         if "text" in _content_type:
#             if "plain" in _content_subtype:
#                 _has_plain_flg = True
#                 _text += _part.get_payload()
#             elif "html" in _content_subtype:
#                 if not _has_plain_flg:  # if this html is not the replacement of text
#                     _text += text4html(_part.get_payload())
#             elif "related" in _content_subtype:
#                 _text, _has_plain_flg_change = parse_related(_part_all)
#                 _has_plain_flg = _has_plain_flg | _has_plain_flg_change
#             else:
#                 _text += _part.get_payload()
#     return _text, _has_plain_flg


def parse_text(_msg):
    _text = ""
    _has_plain_flg = False
    # print(type(_msg))
    if _msg.get_content_maintype() == "text":
        # charset = _msg.get_content_charset()
        # print(charset)
        # if charset:
        #     _part_str = _msg.get_payload(decode=True).decode(charset)
        # else:
        #     try:
        #         _part_str = _msg.get_payload(decode=True).decode("utf-8")
        #     except UnicodeError as e:
        #         print(e)
        #         try:
        #             _part_str = _msg.get_payload(decode=True).decode("cp1252")
        #         except UnicodeError as e:
        #             print(e)
        #             exit(-1)

        _part_str = _msg.get_payload()
        if _msg.get_content_subtype() == "plain":
            _has_plain_flg = True
            _text = _part_str
        elif _msg.get_content_subtype() == "html":
            # print(_part_str)
            _text = text4html(_part_str)

    # print(_text)
    return _text, _has_plain_flg


# TODO: check remote content
def get_mixed(_msg):
    _text = ""
    if _msg.is_multipart():
        # charset = _msg.get_content_charset()
        # print(charset)
        # if charset:
        #     _msg = _msg.get_payload(decode=True).decode(charset)
        # print(_msg)
        for _part in _msg.get_payload():
            if _part.is_multipart():
                _text += get_mixed(_part)
            else:
                _text_str, _has_plain_flg = parse_text(_part)
                if _has_plain_flg:
                    _text = _text_str
                else:
                    _text += _text_str
    else:
        _text, _has_plain_flg = parse_text(_msg)

    return _text


def extract_email_addr(_email_str):
    if not isinstance(_email_str, str):
        _email_str = str(_email_str)
    _email_str = list(filter(None, re.split(r"[<>]", _email_str.strip())))[-1]
    return _email_str


def get_all_info(_filename):
    # print(_filename)
    try:
        _msg = open(_filename, "r", encoding="utf-8", errors="ignore").read()
    except:
        try:
            # print("cp1252")
            _msg = open(_filename, "r", encoding="cp1252", errors="ignore").read()
        except:
            # print("charset")
            charset = chardet.detect(open(_filename, "rb").read())["encoding"]
            print(charset)
            _msg = open(_filename, "r", encoding=charset, errors="ignore").read()

    _msg = email.message_from_string(_msg)
    charset = _msg.get_content_charset()
    if charset:
        try:
            _msg = open(_filename, "r", encoding=charset, errors="ignore").read()
        except:
            _msg = open(_filename, "r", encoding="cp1252", errors="ignore").read()
        _msg = email.message_from_string(_msg)
        # print(_filename, f)
    # for k, v in _msg.items():
    #     print(k, v)
    _text = get_mixed(_msg)
    _email_from = extract_email_addr(_msg["from"]) if _msg["from"] else ""
    _email_msg_id = extract_email_addr(_msg["message-id"]) if _msg["message-id"] else ""
    _email_to = extract_email_addr(_msg["to"]) if _msg["to"] else ""
    _email_reply = extract_email_addr(_msg["reply-to"]) if _msg["reply-to"] else ""
    if not _email_reply:
        _email_reply = extract_email_addr(_msg["return-path"]) if _msg["return-path"] else ""
    _email_cc = extract_email_addr(_msg["cc"]) if _msg["cc"] else ""
    _email_bcc = extract_email_addr(_msg["bcc"]) if _msg["bcc"] else ""

    _email_sbj = _msg["subject"] if _msg["subject"] else ""

    _email_receives = _msg.get_all("received") if _msg["received"] else []
    _email_first_receive = _email_receives[-1] if _email_receives else ""
    _email_last_receive = _email_receives[0] if _email_receives else ""


    # _text = parse_text(_text)
    return _text, _email_from, _email_msg_id, _email_to, \
           str(_email_sbj), _email_reply, _email_cc, \
           _email_bcc, _email_receives, _email_last_receive, \
           _email_first_receive


def get_time(_time_str):
    _time_pattern = re.compile(r"\d{1,2} [a-zA-Z]{3} \d{4} \d{1,2}:\d{1,2}:\d{1,2}")
    _tz_pattern = re.compile(r"\A(([-\+](\d{2}|\d{4}))|[a-zA-Z]{1,5})\Z")
    _time_str_lst = list(filter(None, re.split(r"(\d{1,2} [a-zA-Z]{3} \d{4} \d{1,2}:\d{1,2}:\d{1,2})",
                                               _time_str.strip())))
    while _time_str_lst:
        _x = _time_pattern.search(_time_str_lst.pop(0).strip())
        if _x:
            _time = _x.group(0)
            while _time_str_lst:
                _tz_lst = list(filter(None, re.split(r"[ ()\n<>{}\[\]]", _time_str_lst.pop(0).strip())))
                while _tz_lst:
                    _y = _tz_pattern.search(_tz_lst.pop(0).strip())
                    if _y:
                        _time += " " + _y.group(0)
                        return _time
    return None


def time_duration(_earlier_time_str, _later_time_str):
    _e_time = get_time(_earlier_time_str)
    _l_time = get_time(_later_time_str)
    if not _e_time or not _l_time:
        return 0, 0

    try:
        _earlier_time = dateparser.parse(_e_time, settings={'RETURN_AS_TIMEZONE_AWARE': True})
        if not _earlier_time:
            try:
                _earlier_time = dateutil.parser.parse(_e_time)
            except:
                return 0, 1
    except:
        print(_earlier_time_str)
        print(_e_time)
        settings.write_finished_docs()
        exit(-1)

    try:
        _later_time = dateparser.parse(_l_time, settings={'RETURN_AS_TIMEZONE_AWARE': True})
        if not _later_time:
            try:
                _later_time = dateutil.parser.parse(_l_time)
            except:
                return 0, 1
    except:
        print(_later_time_str)
        print(_l_time)
        settings.write_finished_docs()
        exit(-1)

    try:
        _span_time = (_later_time - _earlier_time).seconds
    except:
        print("failed get spam_time")
        print(_later_time)
        print(_earlier_time)
        settings.write_finished_docs()
        exit(-1)
    else:
        _wrong_time = 0
        if _later_time < _earlier_time:
            _wrong_time = 1
        return _span_time, _wrong_time


def resolve_features(_text, _email_sbj, _email_receives, _email_first_receive, _email_last_receive,
                     _email_from, _email_msg_id, _email_reply, _email_to, _email_cc, _email_bcc,
                     _greek_alphabet_dict, _wired_char_set):
    _weird_char = 0
    _weird_addr = 0
    _weird_target = 0
    _weird_content = 0
    _weird_msg_id = 0
    _weird_sbj = 0
    _wrong_time = 0
    _span_time = 0

    if not _email_to and not _email_cc and _email_bcc:
        _weird_target = 1
    for _ in _email_from:
        _weird_char |= 1 if _ in _greek_alphabet_dict else 0
    for _ in _email_reply:
        _weird_char |= 1 if _ in _greek_alphabet_dict else 0

    for _ in _email_sbj:
        _weird_sbj |= 1 if _ in _greek_alphabet_dict or _wired_char_set else 0
    if _email_from != _email_reply:
        _weird_addr = 1
    if not _text:
        _weird_content = 1
    if _email_from and _email_msg_id:
        if _email_from.strip().split("@")[-1] != _email_msg_id.strip().split("@")[-1]:
            _weird_msg_id = 1
    if _email_first_receive and _email_last_receive:
        _span_time, _wrong_time = time_duration(_email_first_receive, _email_last_receive)

    _servers_count = len(_email_receives)
    return _weird_char, _weird_addr, _weird_sbj, _weird_target, _weird_content, \
           _weird_msg_id, _servers_count, _span_time, _wrong_time


def parse_email_true_value(_file_line, _resource_path):
    # spam.. /data /inmail.1
    # ham.. /data /inmail.2
    _e_lst = list(filter(None, re.split(r"[\. /]", _file_line.strip())))
    _email_true_value_dict = {
        "spam": 1 if _e_lst[0] == "spam" else 0,
        "path": join(_resource_path, _e_lst[1], ".".join(_e_lst[-2:])),
        # "doc_name": ".".join(_e_lst[-2:]),
        "doc_id": _e_lst[-1]
    }
    return _email_true_value_dict


if __name__ == "__main__":
    settings.init()

    text, \
    email_from, email_msg_id, \
    email_to, \
    email_sbj, \
    email_reply, \
    email_cc, \
    email_bcc, \
    email_receives, \
    email_last_receive, email_sent_time = get_all_info("inmail.80")
    # print(email_sent_time)
    # print(email_last_receive)
    # print(email_sbj)
    print(text)
    # if "Does" in text:
    #     print("y")
    # for char in text:
    #     print(char)
    # print(email_from)
    # print(email_msg_id)
    # print(email_to)
    # print(email_reply)
    # print(email_cc)
    # print(email_bcc)
    # print(email_from.strip().split("@")[-1])
    print(resolve_features(text, email_sbj, email_receives, email_sent_time, email_last_receive,
                           email_from, email_msg_id, email_reply, email_to, email_cc, email_bcc,
                           settings.greek_alphabet, settings.wired_char_set))
