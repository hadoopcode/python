from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
from summa import keywords
from string import punctuation
import numpy as np
import re
import pandas as pd
import nltk


def str_func(x):
    df = x.to_frame(name='tfidf')
    df_filter = df[df['tfidf'] != 0].T
    # np.argsort(df_filter.toarray(), axis=1)[:, -5:]
    word_tidf_list = list()
    for index, row in df_filter.iteritems():
        word_tidf_list.append(index + '/' + str(row[0]))
    return '|'.join(word_tidf_list)


def get_keywords(x, ratio=0.2):
    mix = x['keywords_tfidf']
    key_tdidf_list = mix.split('|')
    series = pd.Series(data=key_tdidf_list)

    split_df = series.str.rsplit(r'/', expand=True, n=1).rename(columns={0: 'word', 1: 'tfidf'})
    split_df.sort_values(by='tfidf', ascending=False, inplace=True)
    split_df.set_index('tfidf', inplace=True)
    # words = split_df['word'].tolist()
    length = len(split_df['word'].tolist()) * ratio
    if length < 1:
        return '/0.0000'
    result = split_df.head(n=int(length))
    str_result = '|'.join(['/'.join([value['word'], index[:6]]) for index, value in result.iterrows()])
    print(str_result)
    return str_result


def create_matrix(df):
    item = dict()
    for index, row in df.iterrows():
        item[index] = {i.split('/')[0]: i.split('/')[1] for i in row['all_keywords'].split('|')}
    words = list()
    for v in item.values():
        words.extend(list(v.keys()))
    keywords = list(filter(lambda x: x is not '', set(words)))
    keyword_matrix = pd.DataFrame(data=np.zeros((df.shape[0], len(keywords))), columns=keywords)
    item = dict((key, value) for key, value in item.items() if key is not '')
    for key, value in item.items():
        for k, v in value.items():
            if k is not '':
                keyword_matrix[k][key] = v
    return keyword_matrix


def get_keywords_tfidf(text):
    count_vectorizer = CountVectorizer(tokenizer=re.compile('\\|').split)
    text_vectorizer = count_vectorizer.fit_transform(text)
    transformer = TfidfTransformer()
    tfidf = transformer.fit_transform(text_vectorizer)
    tfid_df = pd.DataFrame(tfidf.toarray())
    # 获取词袋模型中的所有词语（格式为list) ,作为数据框的columns
    tfid_df.columns = count_vectorizer.get_feature_names()
    df = tfid_df.apply(str_func, axis=1).to_frame(name='keywords_tfidf')
    all_keyword_df = df.apply(get_keywords, axis=1).to_frame(name='all_keywords')
    matrix = create_matrix(all_keyword_df)
    return matrix


def normalise(word):
    lemmatizer = nltk.WordNetLemmatizer()
    word = lemmatizer.lemmatize(word)
    return word


def get_keywords_summa(text):
    keywords_list = keywords.keywords(text, split=True, ratio=0.2, scores=False)
    words = [normalise(word.lower()) for word in keywords_list if len(word) > 3 and not re.match(word, punctuation)]
    for word in words:
        if len(word.split(' ')) <= 1:
            pos = nltk.pos_tag([word])[0][1]
            if pos not in ['NN', 'NNS', 'NNP', 'NNPS']:
                words.remove(word)
                continue
        else:
            if word.split(' ')[0] in ['a', 'an', 'aa', 'aan']:
                words.remove(word)
                words.append(' '.join(word.split(' ')[1:]).strip())
    print('|'.join(words))
    return '|'.join(words)
