SELECT Genres.Genre_number as Genero , COUNT(*) as Cantidad FROM Genres, Show, Show_genres WHERE Show_genres.ID_Genre = Genres.ID AND Show_genres.ID_Show = Show.ID GROUP BY Genre_number ORDER BY Cantidad DESC 