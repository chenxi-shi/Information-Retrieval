def init():
    global greek_alphabet
    greek_alphabet = {
        u'\u0391': 'Alpha',
        u'\u0392': 'Beta',
        u'\u0393': 'Gamma',
        u'\u0394': 'Delta',
        u'\u0395': 'Epsilon',
        u'\u0396': 'Zeta',
        u'\u0397': 'Eta',
        u'\u0398': 'Theta',
        u'\u0399': 'Iota',
        u'\u039A': 'Kappa',
        u'\u039B': 'Lamda',
        u'\u039C': 'Mu',
        u'\u039D': 'Nu',
        u'\u039E': 'Xi',
        u'\u039F': 'Omicron',
        u'\u03A0': 'Pi',
        u'\u03A1': 'Rho',
        u'\u03A3': 'Sigma',
        u'\u03A4': 'Tau',
        u'\u03A5': 'Upsilon',
        u'\u03A6': 'Phi',
        u'\u03A7': 'Chi',
        u'\u03A8': 'Psi',
        u'\u03A9': 'Omega',
        u'\u03B1': 'alpha',
        u'\u03B2': 'beta',
        u'\u03B3': 'gamma',
        u'\u03B4': 'delta',
        u'\u03B5': 'epsilon',
        u'\u03B6': 'zeta',
        u'\u03B7': 'eta',
        u'\u03B8': 'theta',
        u'\u03B9': 'iota',
        u'\u03BA': 'kappa',
        u'\u03BB': 'lamda',
        u'\u03BC': 'mu',
        u'\u03BD': 'nu',
        u'\u03BE': 'xi',
        u'\u03BF': 'omicron',
        u'\u03C0': 'pi',
        u'\u03C1': 'rho',
        u'\u03C3': 'sigma',
        u'\u03C4': 'tau',
        u'\u03C5': 'upsilon',
        u'\u03C6': 'phi',
        u'\u03C7': 'chi',
        u'\u03C8': 'psi',
        u'\u03C9': 'omega',
    }
    global wired_char_set
    wired_char_set = {"@", "$", "*", "❤", "★", "+", "!",
                      "[", "]", "{", "}", "#", "=", "`"}
    global finished_doc_id
    finished_doc_id = set()
    with open("finished_docs.txt", "r", errors="replace") as _ff:
        for _line in _ff:
            finished_doc_id.add(_line.strip())

    global stop_words_set
    stop_words_set = set()
    with open("stoplist.txt", "r", errors="replace") as sf:
        for _line in sf:
            stop_words_set.add(_line.strip())

def write_finished_docs():
    global finished_doc_id
    with open("finished_docs.txt", "w", errors="replace", encoding='utf8') as _ff:
        for _doc_id in finished_doc_id:
            _ff.write("{}\n".format(_doc_id))

if __name__ == "__main__":
    x = "Σ"
    y = "E"
    z = "❤"
    if z in wired_char_set:
        print('Y')

