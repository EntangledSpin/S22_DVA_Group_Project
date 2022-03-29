from names_dataset import NameDataset

# first_names - NOT CURRENTLY IN USE
#      arguments: None
#      returns:   all_names_final - a list of english first names
#
def first_names():
    nd = NameDataset()

    male_names = nd.get_top_names(n=1000000, gender="Male", country_alpha2='US')
    female_names = nd.get_top_names(n=1000000, gender="Female", country_alpha2='US')

    male_names = male_names["US"]["M"]
    female_names = female_names["US"]["F"]

    all_names = male_names + female_names
    all_names = set(all_names)
    all_names = list(all_names)
    all_names = [name.lower() for name in all_names]
    all_names.sort()

    letter_lst = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k",
                  "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v",
                  "w", "x", "y", "z"]

    all_names_final = [name for name in all_names if name[0] in letter_lst]

    return all_names_final
