import logging

logger = logging.getLogger(__name__)
table = 'yt_api'

def insert_rows(cursor,conn, schema,row):

    try:
        if schema == 'staging':
            
            video_id = 'video_id'

            cursor.execute(
                f"""INSERT INTO {schema}.{table}(
                "video_id", "title", "published_at", "duration", "view_count", "like_count", "comment_count"
            ) VALUES(%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s,
              %(commentCount)s);""",
                row
            )
        else: 
            video_id = 'video_id'

            cursor.execute(
                f"""INSERT INTO {schema}.{table}(
                "video_id", "title", "published_at", "duration","video_type", "view_count", "like_count", "comment_count"
            ) VALUES (%(video_id)s, %(title)s, %(published_at)s, %(duration)s,%(video_type)s, %(view_count)s, %(like_count)s,
              %(comment_count)s);""",
                row
            )
        conn.commit()

        logger.info(f"Inserted row with video_id: {row[video_id]} into {schema}.{table}")

    except Exception as e:
        logger.error(f"Error inserting row with video_id: {row[video_id]} into {schema}.{table}: {e}")
        raise e
    
def update_rows(cursor, conn, schema, row):

        try:
            #staging 
            if schema == 'staging':
                video_id = 'video_id'
                published_at = 'publishedAt'
                title = 'title'
                duration = 'duration'
                view_count = 'viewCount'
                like_count = 'likeCount'
                comment_count = 'commentCount'

            #core
            else:
                video_id = 'video_id'
                published_at = 'published_at'
                title = 'title'
                duration = 'duration'
                view_count = 'view_count'
                like_count = 'like_count'
                comment_count = 'comment_count'

                cursor.execute(
                    f"""UPDATE {schema}.{table}
                    SET "title" = %({title})s,
                      "view_count" = %({view_count})s,
                      "like_count" = %({like_count})s,
                      "comment_count" = %({comment_count})s
                    WHERE "video_id" = %({video_id})s AND published_at = %({published_at})s;""",
                    row
                )

                conn.commit()

                logger.info(f"Updated row with video_id: {row[video_id]}")

        except Exception as e:
            logger.error(f"Error updating row with video_id: {row[video_id]}: {e}")
            raise e
        
def delete_rows(cursor, conn, schema, ids_to_delete):

    try:
        ids_to_delete = f"""({', '.join(f"'{id}'" for id in ids_to_delete)})"""

        cursor.execute(
            f"""DELETE FROM {schema}.{table}
            WHERE "video_id" IN {ids_to_delete};"""
            )
    except Exception as e:
        logger.error(f"Error deleting rows with video_ids: {ids_to_delete}: {e}")
        raise e