class MessageEntry(object):
    def __init__(self, mid = 1, body = 'Hello world!', from_field = 'worker@bbh.com', subject = 'Welcome message', \
                 date = '15.09.2015', owner = 'worker@bbh.com', to = 'all@bbh.com', cc = None, bcc = None, folder = ''):
        self.mid = mid
        self.body = body
        self.from_field = from_field
        self.subject = subject
        self.folder = folder
        self.date = date
        self.owner = owner
        self.to = self.format(to)
        self.cc = self.format(cc)
        self.bcc = self.format(bcc)
        self.body_subject_no_stop_words = self.get_clean_subject_body()
        self.term_frequency = None
      
    def __str__(self):
        separator = '\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n'
        return separator + \
               'mid: ' + str(self.mid) + '\t\tdate: ' +  str(self.date) + '\t\towner: ' + str(self.owner) + \
               '\nFROM: ' + str(self.from_field) + \
               '\nTO: ' + str(self.to) + '\t\tCC: ' + str(self.cc) + '\t\tBCC: ' + str(self.bcc) + \
               '\nSUBJECT: ' + str(self.subject) + '\nBODY: ' + str(self.body) + \
               '\nterm frequency: ' + str(self.term_frequency) + \
               '\nfolder: ' + str(self.folder) + separator + '\n'
        
    def __repr__(self):
        return self.__str__()
    
    def get_clean_subject_body(self):
        tmp = self.subject + ' ' + self.body
        import re
        temp_result = re.sub('\s+', ' ', tmp.lower()).strip()
        stop_words = ["about", "all", "am", "an", "and", "are", "as", "at", "be", "been", "but", "by", "can", "cannot", "did", "do", "does", "doing", "done", "for", "from", "had", "has", "have", "having", "if", "in", "is", "it", "its", "of", "on", "that", "the", "they", "these", "this", "those", "to", "too", "want", "wants", "was", "what", "which", "will", "with", "would"]
        word_list = []
        for word in temp_result.split():
            if word not in stop_words:
                word_list.append(word)
        return ' '.join(word_list)
            
        
    
    def __attrs(self):
        return (self.mid, self.body, self.from_field, self.subject, self.date, self.owner, self.to, self.cc,\
                self.bcc)
     
    def __eq__(self, other):
        return isinstance(other, MessageEntry) and self.__attrs() == other.__attrs()

    def __hash__(self):
        return hash(self.__attrs())

    def format(self, text):
        if text is not None:
            text = tuple(text.split(','))
        return text