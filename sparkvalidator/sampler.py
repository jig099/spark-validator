### sample APIs###
# 1. random_sampling()
# 2. stratified_sampling()
# 3. systematic_sampling()
# 4. clustered_sampling()
class sampler:
    def __init__(self, df, seed):
        self.seed = seed
        self.df = df

    """
        sample function will take in either a dataframe
        and return sampled data back to the user

        :param with_replacement: sample with replacement or not (default False)
        :param fraction: fraction of rows to generate    
        :return: return sampled dataset
    """
    def random_sampling(self, with_replacement, fraction):
       
        return self.df.sample(with_replacement,fraction, self.seed)

    """
        sample function will take in either a dataframe
        and return sampled data back to the user

        :param col_name: column name from DataFrame
        :param fractiosn: dictionary type takes key and value
        :return: return sampled dataset
    """
    def stratified_sampling(self, col_name, fractions):
        
        return self.df.sampleBy(col_name, fractions, self.seed)