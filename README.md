# d2hl - Dupes to Hardlinks

**NOTE**: As of 2024-06-23 the Go import url of this repo has changed to:
`pkg.i-no.de/pkg/d2hl`. Issues, merge requests etc should be filed at its new
source location, https://codeberg.org/klausman/d2hl

A very simple and brittle tool to turn duplicate files (as in contents) into
hardlinks.

Bugs this *definitely* has (there are likely more):

- It will happily cross filesystem boundaries
- It is entirely ignorant of symlinks
- Its rename-replace-delete logic is racy

In other words: if you use this, you are perfectly fine with it destroying
all of your data. DO NOT USE.
