
import numpy as np
import pandas as pd
class song_similarity_recommender():
    def __init__(self):
        self.train_data = None
        self.user_id = None
        self.item_id = None
        self.cooccurence_matrix = None
        self.songs_dict = None
        self.rev_songs_dict = None
        self.item_similarity_recommendations = None
        
    def get_item_users(self, item):
        item_data = self.train_data[self.train_data[self.item_id] == item]
        item_users = set(item_data[self.user_id].unique())
            
        return item_users
        
    def get_all_items_train_data(self):
        all_items = list(self.train_data[self.item_id].unique())
            
        return all_items
        
    def construct_cooccurence_matrix(self, user_songs, all_songs, train_data):

        user_songs_users = []        
        for i in range(0, len(user_songs)):
            user_songs_users.append(self.get_item_users(user_songs[i]))

        cooccurence_matrix = np.matrix(np.zeros(shape=(len(user_songs), len(all_songs))), float)
            
        for i in range(0, len(all_songs)):

            songs_i_data = train_data[train_data[self.item_id] == all_songs[i]]
            users_i = set(songs_i_data[self.user_id].unique())
            
            for j in range(0, len(user_songs)):       

                users_j = user_songs_users[j]

                users_intersection = users_i.intersection(users_j)

                if len(users_intersection) != 0:

                    users_union = users_i.union(users_j)
                    
                    cooccurence_matrix[j,i] = float(len(users_intersection))/float(len(users_union))
                else:
                    cooccurence_matrix[j,i] = 0
                    
        return cooccurence_matrix


    def generate_top_recommendations(self, user, cooccurence_matrix, all_songs, user_songs, train_data):
        print("Non zero values in cooccurence_matrix :%d" % np.count_nonzero(cooccurence_matrix))
        user_sim_scores = cooccurence_matrix.sum(axis=0)/float(cooccurence_matrix.shape[0])
        user_sim_scores = np.array(user_sim_scores)[0].tolist()

        sort_index = sorted(((e,i) for i,e in enumerate(list(user_sim_scores))), reverse=True)
    
        columns = ['rank', 'song', 'year']
        df = pd.DataFrame(columns=columns)
         
        rank = 1 
        for i in range(0, len(sort_index)):
            if ~np.isnan(sort_index[i][0]) and all_songs[sort_index[i][1]] not in user_songs and rank <= 10:
                song = all_songs[sort_index[i][1]]
                year = train_data[train_data[self.item_id] == song]['year'].values[0]  
                df.loc[len(df)] = [rank, song, year]
                rank = rank + 1
        
        if df.shape[0] == 0:
            print("The current user has no songs for training the item similarity based recommendation model.")
            return -1
        else:
            return df

 

    def create(self, train_data, user_id, item_id):
        self.train_data = train_data
        self.user_id = user_id
        self.item_id = item_id
    

    def get_similar_items(self, item_list):
        
        user_songs = item_list

        all_songs = self.get_all_items_train_data()
        
        print("no. of unique songs in the training set: %d" % len(all_songs))
         

        cooccurence_matrix = self.construct_cooccurence_matrix(user_songs, all_songs,self.train_data)

        user = ""
        df_recommendations = self.generate_top_recommendations(user, cooccurence_matrix, all_songs, user_songs,self.train_data)
         
        return df_recommendations