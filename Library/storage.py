import os

class store():

    def __init__(self,datasource=None):
        self.__pairs = {}
        self.datasource = datasource
        self.__make_pairs()

    # it makes key value pairs with keys as line number and value as the line, by iterating through every line of every file when an input directory is being provided.
    # Used when you define first mapper and inputs to that mapper have to be given from files in a directory
    def __make_pairs(self):

        def check_valid_file(file_path):
            def is_txt_file(file_path_):
                # Split the file name and its extension
                _, file_extension = os.path.splitext(file_path_)

                # Check if the file extension is '.txt'
                return file_extension.lower() == '.txt'

            if (os.path.isfile(file_path) and is_txt_file(file_path)):
                return True
            return False

        def make_pairs_array():

            assert os.path.isdir(self.datasource), "Error: 'self.datasource' must be a valid directory"
            id = 0
            for filename in os.listdir(self.datasource):

                filepath = os.path.join(self.datasource, filename)

                # checking if it is a .txt file.
                if check_valid_file(filepath):

                    with open(filepath, 'r') as file:    
                        for line in file:
                            line = line.strip('\n')
                            id +=1 
                            self.__pairs[id] = line
        
        if(self.datasource!=None):
            make_pairs_array()


    def emit(self,key,value):

        if key not in self.__pairs:
            self.__pairs[key] = []

        if isinstance(value, list):
            self.__pairs[key].extend(value)
        else:
            self.__pairs[key].append(value)

    def get_key_counts(self):
        counts = {}
        # print("inside get_key_counts\n")
        # print("self.pairs",self.__pairs)
        
        for key in self.__pairs.keys():
            value = self.__pairs[key]
            counts[key] = len(value)
        return counts

    def get_keys(self):
        self.__pairs = {key: self.__pairs[key] for key in sorted(self.__pairs.keys())}
        return list(self.__pairs.keys())

    def is_key_present(self, key):
        return key in self.__pairs

    def get_key_values(self, key):
        return self.__pairs.get(key, None)


        
    


        



    