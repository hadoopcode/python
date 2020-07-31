import itertools
import re
from string import punctuation

import nltk
from nltk.corpus import stopwords


class ProcessText(object):

    def leaves(self, toks, grammar, pos):
        chunker = nltk.RegexpParser(grammar)
        postoks = nltk.tag.pos_tag(toks)
        tree = chunker.parse(postoks)
        # tree.draw()
        for subtree in tree.subtrees(lambda t: t.label() == pos):
            yield subtree.leaves()

    def get_term(self, value):
        for i, j in value:
            print(i)
            yield i

    # 提取指定词性的单词
    def fields_leaves(self, toks, grammar, pos):

        return [" ".join([self.normalise(w) for w, p in leave if self.acceptable_word(w)]) for leave in
                self.leaves(toks, grammar, pos)]

    # result = list()
    # leaves = self.leaves(dic_toks['a'], grammar, pos)
    #
    # b = {key: self.get_term(self.leaves(value, grammar, pos)) for key, value in
    #      dic_toks.items()}
    # return b

    # 过滤单词对的规则
    def acceptable_word(self, word):
        return bool(not re.match(r'[{}]'.format(punctuation), word) and not re.search(r'\d',
                                                                                      word) and word.lower() not in stopwords.words(
            'english') and 2 <= len(word) <= 40 and word.strip() != r'e.g')

    # 过滤不符合规则单词、停用词、提取词干
    def filter_normalise_word(self, kwargs):
        return {key: [self.normalise(w.lower()) for w in value if self.acceptable_word(w)] for key, value in
                kwargs.items()}

    # 提取词干
    def normalise(self, word):
        lemmatizer = nltk.WordNetLemmatizer()
        word = lemmatizer.lemmatize(word)
        return word

    # 分词
    def word_from_list_tokenize(self, test):
        return nltk.word_tokenize("".join(test))
        # return {key: nltk.word_tokenize("".join(value)) for key, value in kwargs.items()}


def extract_phrase(text):
    pt = ProcessText()
    tokenize_ab_ti = pt.word_from_list_tokenize(text)
    grammar = r"""
                    NBAR:
                        {<NN.*|JJ>*<NN.*>}  # Nouns and Adjectives, terminated with Nouns

                    NP:
                        {<NBAR>}
                        {<NBAR><IN><NBAR>}  # Above, connected with in/of/etc...;IN代表介词
                """
    np_leaves = pt.fields_leaves(tokenize_ab_ti, grammar, "NP")
    return list(filter(lambda x: x and x.strip(), np_leaves))
