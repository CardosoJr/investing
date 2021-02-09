import fundamentus 
import pandas as pd

class Getter:
    def Extract_Data(self):
        result = fundamentus.get_data()
        df = pd.DataFrame.from_dict(result, orient = 'index')
        return df


