# d2hl - Dupes to Hardlinks

A very simple and brittle tool to turn duplicate files (as in contents) into
hardlinks.

Bugs this *definitely* has (there are likely more):

- It will happily cross filesystem boundaries
- It is entirely ignorant of symlinks
- Its rename-replace-delete logic is racy

In other words: if you use this, you are perfectly fine with it destroying
all of your data. DO NOT USE.
