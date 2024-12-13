import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, Embedding, Flatten, Dense, Concatenate
from modules.data_loading import read_data_from_google_drive
from modules.recommend_user import recommend_songs_for_user
from modules.similar_songs_recommender import song_similarity_recommender

song_data_file_id = '12WGL8DcXJylPK9ZxKWdw0PF271KqT7Gk'
song_data = read_data_from_google_drive(song_data_file_id)


triplets_data_id = '19QbIStUyUdjhIOnxJLWSIwwL2jYGQh4I'
triplets_data  = read_data_from_google_drive(triplets_data_id)

print("No. of songs:",len(song_data))
print("No of Users:",len(triplets_data))


merged_data = pd.merge(triplets_data, song_data, on='song_id', how='left')

user_ids = merged_data['user_id'].unique()
user_id_map = {user_id: i for i, user_id in enumerate(user_ids)}
song_ids = merged_data['song_id'].unique()
song_id_map = {song_id: i for i, song_id in enumerate(song_ids)}

merged_data['user_idx'] = merged_data['user_id'].map(user_id_map)
merged_data['song_idx'] = merged_data['song_id'].map(song_id_map)

train_data, val_data = train_test_split(merged_data, test_size=0.2, random_state=42)

embedding_dim = 50

user_input = Input(shape=(1,))
song_input = Input(shape=(1,))

user_embedding = Embedding(input_dim=len(user_ids), output_dim=embedding_dim)(user_input)
user_embedding = Flatten()(user_embedding)

song_embedding = Embedding(input_dim=len(song_ids), output_dim=embedding_dim)(song_input)
song_embedding = Flatten()(song_embedding)

concat = Concatenate()([user_embedding, song_embedding])

fc1 = Dense(128, activation='relu')(concat)
output = Dense(1, activation='sigmoid')(fc1)

model = Model(inputs=[user_input, song_input], outputs=output)
model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
model.summary()

X_train = [train_data['user_idx'], train_data['song_idx']]
y_train = train_data['listen_count']

X_val = [val_data['user_idx'], val_data['song_idx']]
y_val = val_data['listen_count']

history = model.fit(X_train, y_train, validation_data=(X_val, y_val), epochs=1, batch_size=64)

user_id = 'b80344d063b5ccb3212f76538f3d9e43d87dca9e'

recommended_songs = recommend_songs_for_user(user_id, model, user_id_map, song_ids)

print(f"\nRecommended Songs for User {user_id}:")
for i, (song_id, prediction) in enumerate(recommended_songs[:10], 1):
    song_title = song_data.loc[song_data['song_id'] == song_id, 'title'].iloc[0]
    print(f"{i}. {song_title}")

ir = song_similarity_recommender()

year_from = 1990
year_to = 2000

cropped_data = merged_data[(merged_data['year'] >= year_from) & (merged_data['year'] <= year_to)]

cropped_data['song'] = cropped_data['title']+' - '+cropped_data['artist_name']

ir.create(cropped_data, 'user_id', 'song')

similar_songs = ir.get_similar_items(['Holes To Heaven - Jack Johnson'])

print("Similar Songs:")
print(similar_songs)
