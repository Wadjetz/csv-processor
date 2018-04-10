On dispose d'un fichier CSV, selon le modèle suivant: 
input.csv : userId,itemId,rating,timestamp

On souhaite construire 3 CSV de la façon suivante: 
- aggratings.csv : userIdAsInteger,itemIdAsInteger,ratingSum 
- lookupuser.csv : userId,userIdAsInteger 
- lookup_product.csv : itemId,itemIdAsInteger

où: 
- userId : identifiant unique d'un utilisateur (String) 
- itemId : identifiant unique d'un produit (String) 
- rating : score (Float) 
- timestamp : timestamp unix, nombre de millisecondes écoulées depuis 1970-01-01 minuit GMT (Long/Int64) 
- userIdAsInteger : identifiant unique d'un utilisateur (Int) 
- itemIdAsInteger : identifiant unique d'un produit (Int) 
- ratingSum : Somme des ratings pour le couple utilisateur/produit (Float)

Ecrire un programme dans le langage Scala respectant les contraintes suivantes: 
- dans agg_rating.csv, les couples utilisateur/produit sont uniques 
- Les userIdAsInteger (tout comme les productIdAsInteger) sont des entiers consécutifs, le premier indice étant 0. 
- une pénalité multiplicative de 0.95 est appliquée au rating pour chaque jour d'écart avec le timestamp maximal de input.csv 
- On ne souhaite conserver que les ratings > 0.01
