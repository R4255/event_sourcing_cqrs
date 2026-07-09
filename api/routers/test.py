from collections import defaultdict


def isAnagram(s: str, t: str) -> bool:
    def dict_form(string: str):
        count_of_alpha = defaultdict(int)
        for char in string:
            count_of_alpha[char] += 1
        return count_of_alpha
    print(dict_form(s))
    print(dict_form(t))
    if dict_form(s) == dict_form(t):
        return True
    return False

isAnagram("anagram", "nagaram")