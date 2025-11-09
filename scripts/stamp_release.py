import pathlib, shutil, datetime
src = pathlib.Path("data/processed")
dst = pathlib.Path("data/releases") / datetime.datetime.utcnow().strftime("%Y-%m-%d")
dst.mkdir(parents=True, exist_ok=True)
for f in ["papers.csv","citations.csv","authors.csv"]:
    shutil.copy(src/f, dst/f)
print("Release staged at", dst)
