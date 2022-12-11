import re
import warnings
from functools import reduce

def parse(title: str):
    # according to https://en.wikipedia.org/wiki/Most_common_words_in_English

    most_common = [
        "the", "be", "to", "of", "and", "a", "in",
        "that", "have", "I", "it", "for", "not",
        "on", "with", "he", "as", "you", "do", "at",
        "this", "but", "his", "by", "from", "they",
        "we", "say", "her", "she", "or", "an", "will",
        "my", "one", "all", "would", "there", "their",
        "what", "so", "up", "out", "if", "about",
        "who", "get", "which", "go"
    ]    
    
    ptrn = reduce(
        (lambda x,y : x+r"b?y? ("+y+r") |"),
        most_common,
        r""
        )
    ptrn = ptrn[:-1]
    punct = r"[\,\.\!\?\;\:\-\(\)\[\]\{\}\'\"\&\%\$\#\@\*\+\=\/\\\|\<\>\~\`\^\_]"
    parsed = re.sub(punct, ' ', title)
    parsed = re.sub(ptrn, ' ', parsed)
    parsed = re.sub(r"(the)", "", parsed) # leftover the's
    return parsed.lower().replace('  ', ' ').strip()

def irreg_split(df, column: str):
    maxlen = df[column].str.split(' ').apply(len).max()
    tokens = df.apply(lambda row: row[column].split(' ') + (maxlen-len(row[column].split(' ')))*[''], axis=1, result_type='expand')
    return tokens

def tokenize(df, column: str):
    dfc = df.copy()
    dfc["temp"] = dfc[column].apply(parse)
    tokens = irreg_split(dfc, "temp")
    return tokens

def tokens(df, column: str):
    tk = tokenize(df, column)\
        .stack()\
        .value_counts()\
        .reset_index()\
        .rename(columns={
            'index': 'name',
            0: 'count'
            })
    return tk[tk.name != '']