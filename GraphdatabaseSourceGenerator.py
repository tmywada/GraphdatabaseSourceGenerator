# =================================================================================
#                      Graphdatabase Source Generator (GSG) 
# =================================================================================
#
import hashlib
import re
import argparse
import os
import datetime
import copy

# --- version
version = '0.0.1'

class Utilities:

    df __init__(self):

        # --- folder
        self.folder_save_bitmap = './data/metadata_bitmap_files' # bitmap 
        self.folder_save_metadata = './data/metadata'

        # --- files
        self.file_path_abbreviation = './data/abbreviations.pckl'
        self.file_path_columnar_metadata = './data/columnar_metadata.pckl'
        self.file_path_graph_source = './data/graph_source.json'

        # --- set parameters
        self.with_regex = False
        self.hashfunc_salt = '5963'.encode('utf-8')
        self.delimiter_pattern_token = '_| '
        self.delimiter_term = ' '
        self.character_accepted = '[^a-zA-Z _]'
        self.delimiter_node_value = '__'

        # --- initialization
        self.uids = dict()
        self.nodes = []
        self.edges = []

        # --- load abbreviation
        if os.path.isfile(self.file_path_abbreviation) == False:
            message = f'error: abbrevaition file does not exeists'
            print(message)
            quit()
        else:
            self.abbreviations = pickle.load(open(self.file_path_abbreviation, 'rb'))

    def generate_hashvalue(self, value:str):
        """
        Digest
        0: bucket_idx
        1-5: hashvalue
        6: keep (absorbed)

        bucket_idx is first 8 bits (up to 255)
        value is following 32 bits
        hashvalue itself is 56 bits
        """
        # --- cast data type (bytes)
        if isinstance(value, bytes) == False:
            value = value.lower().encode('utf-8')

        # --- calculate hashvalue
        hash_digest = hashlib.blake2s(
            value,
            salt = self.hashfunc_salt,
            digest_size = 7
        )

        # --- store generated in a bucket
        bucket_idx = hash_digest[0]
        value_hash = int.from_bytes(hashdigest[1:5], 'little') # 32 bit for roaringbitmap

        return (bucket_idx, value_hash)

    def generate_hashvalue_from_datetime(self, value_datetime:datetime.datetime=None):
        """
        """
        # --- check input
        if value_datetime == None:
            value_datetime = datetime.now()

        # --- convert to timestamp
        _timestamp = str(value_datetime.timestamp())

        return self.generate_hashvalue(_timestamp)

    def convert_string_to_hashvalue(self, value:str):
        """
        https://stackoverflow.com/a/16008760
        https://stackoverflow.com/a/37238222
        https://stackoverflow.com/a/67219726    
        """
        # --- binary
        try:
            value_binary = value.lower().encode('utf-8')
        except:
            message = f'error: input cannto be encoded, {value}'
            print(message)
            return None

        # --- generate hashvalue
        bucket_idx, value_hash = self.generate_hashvalue(value_binary)

        return (bucket_idx, value_hash)

    def cleanup_string(self, value:str, lower_letter_conversion:bool=True):
        """
        """
        # --- copy input
        _value = copy.deepcopy(value)

        # --- swap characters not considered
        _value = re.sub(self.character_accepted, ' ', _value)

        # --- lower case (as needed)
        if lower_letter_conversion:
            return _value.lower()
        else:
            return _value

    def tokenize_input_string(self, value:str, min_token_length:int=None, lower_letter_conversion:bool=True):
        """
        https://stackoverflow.com/a/3845449
        """
        # --- clean up input
        _value = self.cleanup_string(value)

        # --- generate tokens (exclude empty entities)
        tokens = [v.strip() for v in re.split(self.delimiter_pattern_token, _value) if v]

        # --- check token length
        if min_token_length is not None:
            tokens = [v for v in tokens if len(v) >= min_token_length]

        return tokens

class Processes(Utilities):

    def generate_terms_vocabularies(self, tokens:list):
        """
        goal: 
        terms -> ['new york', 'washington d.c.', 'first year commission']
        vocabularies -> ['new', 'york', 'washington', 'd.c.', 'first', 'year', 'commission']

        input -> ['new york', 'washington d.c.', 'fyc']
        abbreviation -> fyc: "First Year Commission"
        """
        # --- initialization
        terms = set()
        vocabularies = []
        abbreviations_found = set()
        token_not_seen = set()

        # --- loop
        for token in tokens:

            # --- token is known abbreviation
            if token in self.abbreviations:
                abbreviations_found.add(token)

                # --- keep abbreviation's description as "term"
                _term = self.abbreviations[token]

            # --- token is not in abbreviation (unknown or normal English word)
            else:
                token_not_seen.add(token)

                # --- keep token as "term"
                _term = token

            # --- add _term
            terms.add(_term)

        # --- vocabularies (should be only lower letters)
        for term in terms:
            vocabularies.extend(
                [v.lower().strip() for v in re.split(self.delimiter_term, term) if v]
            )

        # --- distinct vocabularies
        vocabularies = sorted(list(set(vocabularies)))

        # --- generate output
        res = {
            'terms': terms,
            'vocabularies': vocabularies,
            'abbreviations_found': sorted(list(abbreviations_found)),
            'tokens_not_seen': sorted(list(tokens_not_seen))
        }
        return res

    def parse_input_string(self, value:str, lower_letter_conversion:bool=True):
        """
        """
        # --- tokenize
        tokens = self.tokenize_input_string(
            value = value,
            lower_letter_conversion = lower_letter_conversion
        )

        # --- terms & vocabularies
        res = self.generate_terms_vocabularies( tokens )

        # --- add input information
        res['value_input'] = value

        return res

    def generate_terms_vocabularies_from_abbreviations(self):
        """
        """
        for abbrevation,descriptions in self.abbreviations.items():

            # --- collect 
            vocabs = [abbrevation.strip().lower()]

            # --- cleanup description
            _descriptions = re.sub(self.character_accepted, ' ', descriptions)

            # --- split and extend vocabs
            vocabs.extend(
                [v.strip() for v in _descriptions.split() if len(v.strip()) > 0]
            )

            for vocab in vocabs:
                _res = self.generate_attributes_node_simple(
                    value = vocab,
                    node_type = 'vocabulary',
                    is_vocabulary = True
                )
                if _res['uid'] not in self.uids:
                    self.uids[_res['uid']] = vocab
                    self.nodes.append( _res['main_content'])        




    def generate_attributes_node(self, value:str, node_type:str):
        """
        """
        # --- concantenate "node_type" and "value" (unique key)
        _value = f'{node_type}{self.delimiter_node_value}{value}'
        uid = str(self.convert_string_to_hashvalue(_value)[1]) # position 0 is bucket_idx
        res = self.parse_input_string(value)

        attributes = {
            'name': value,
            'node_type': node_type,
            'datetime_created': datetime.now().isoformat(),
            'terms': res['terms'],
            'vocabularies': res['vocabularies'],
            'abbreviations_found': res['abbreviations_found']
        }

        output = {
            'main_contents': (uid, attributes),
            'uid': uid,
            'terms': res['terms'],
            'vocabularies': res['vocabularies'],
            'abbreviations_found': res['abbreviations_found']
        }

        return output










