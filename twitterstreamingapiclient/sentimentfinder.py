import re


class CreateKeyword:
    def __init__(self, str):
        self.keyword = " "
        self.searchword = " "
        self.mphrase = []
        self.ophrase = []
        self.rphrase = []
        self.str = str.strip('\n')
        stringtokens = " "
        tokens = 0
        stringtokens = self.str.split("|")

        for stringtoken in stringtokens:
            tokens += 1
            if tokens == 1:
                self.keyword = stringtoken
            elif tokens == 2:
                self.searchword = stringtoken
            elif tokens == 3:
                if len(stringtoken) != 1:
                    mstringtokens = stringtoken.split(",")
                    for mstringtoken in mstringtokens:
                        self.mphrase.append(mstringtoken)
            elif tokens == 4:
                if len(stringtoken) != 1:
                    ostringtokens = stringtoken.split(",")
                    for ostringtoken in ostringtokens:
                        self.ophrase.append(ostringtoken)
            else:
                if len(stringtoken) != 1:
                    rstringtokens = stringtoken.split(",")
                    for rstringtoken in rstringtokens:
                        self.rphrase.append(rstringtoken)


class CreateKeywordList:
    positivekeywordlist = []
    negativekeywordlist = []

    def __init__(self):
        with open('positive.csv', 'r') as f:
            next(f)
            for line in f:
                createkeywordpos = CreateKeyword(line)
                CreateKeywordList.positivekeywordlist.append(createkeywordpos)
        with open('negative.csv', 'r') as f:
            next(f)
            for line in f:
                createkeywordneg = CreateKeyword(line)
                CreateKeywordList.negativekeywordlist.append(createkeywordneg)


class SentimentFinder:

    def __init__(self,str):

        self.str = str
        self.sflag = ' '
        self.keywords = []
        kcount = 0
        mcount = 0
        ocount = 0
        rcount = 0
        pflag = 0
        mflag = 0
        nflag = 0

        for createkeywordobj in CreateKeywordList.positivekeywordlist:
            kcount = len(re.findall('\\b' + createkeywordobj.searchword + '\\b',str,re.IGNORECASE))

            for createkeywordmobj in createkeywordobj.mphrase:
                mcount += len(re.findall('\\b' + createkeywordmobj + '\\b', str, re.IGNORECASE))

            for createkeywordoobj in createkeywordobj.ophrase:
                ocount += len(re.findall('\\b' + createkeywordoobj + '\\b', str, re.IGNORECASE))

            for createkeywordrobj in createkeywordobj.rphrase:
                rcount += len(re.findall('\\b' + createkeywordrobj + '\\b', str, re.IGNORECASE))

            if kcount - mcount - ocount - rcount > 0:
                pflag = 1
                self.keywords.append(createkeywordobj.keyword)
            if mcount > 0:
                mflag = 1
            if ocount > 0:
                nflag = 1

            kcount = 0
            mcount = 0
            ocount = 0
            rcount = 0

        for createkeywordobj in CreateKeywordList.negativekeywordlist:
            kcount = len(re.findall('\\b' + createkeywordobj.searchword + '\\b',str,re.IGNORECASE))

            for createkeywordmobj in createkeywordobj.mphrase:
                mcount += len(re.findall('\\b' + createkeywordmobj + '\\b', str, re.IGNORECASE))

            for createkeywordoobj in createkeywordobj.ophrase:
                ocount += len(re.findall('\\b' + createkeywordoobj + '\\b', str, re.IGNORECASE))

            for createkeywordrobj in createkeywordobj.rphrase:
                rcount += len(re.findall('\\b' + createkeywordrobj + '\\b', str, re.IGNORECASE))

            if kcount - mcount - ocount - rcount > 0:
                nflag = 1
                self.keywords.append(createkeywordobj.keyword)
            if mcount > 0:
                mflag = 1
            if ocount > 0:
                pflag = 1

            kcount = 0
            mcount = 0
            ocount = 0
            rcount = 0

        # print(self.str)

        if mflag == 1:
            self.sflag = "mixed"
        elif pflag == 0 and nflag == 0:
            self.sflag = "neutral"
        elif pflag == 1 and nflag == 1:
            self.sflag = "mixed"
        elif nflag == 1:
            self.sflag = "negative"
        else:
            self.sflag = "positive"

    def getsentiment(self):
        return self.sflag

    def getkeywordlist(self):
        return self.keywords




