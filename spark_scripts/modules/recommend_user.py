import tensorflow as tf
def recommend_songs_for_user(user_id, model, user_id_map, song_ids):

    user_idx = user_id_map.get(user_id)
    if user_idx is None:
        print(f"User ID '{user_id}' not found.")
        return []

    user_idx_tensor = tf.convert_to_tensor([user_idx])


    recommendations = []


    for song_idx in range(len(song_ids)):

        song_idx_tensor = tf.convert_to_tensor([song_idx])


        prediction = model.predict([user_idx_tensor, song_idx_tensor])[0][0]
        recommendations.append((song_ids[song_idx], prediction))


    recommendations.sort(key=lambda x: x[1], reverse=True)

    return recommendations