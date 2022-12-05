import eurostat

from eupol.download.utils import check_dependency, savetmp, funtmpdir, tmpcache, rmcache

@tmpcache
def _toc():
    return eurostat.get_toc_df()

if __name__ == "__main__":
    print(_toc())