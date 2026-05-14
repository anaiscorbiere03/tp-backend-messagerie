### 1. En quoi HTTP convient-il bien à cette application ?

HTTP convient à cette application pour des échanges ponctuels et synchrones entre le client et le serveur comme envoyer un message (requête post), enregistrer un nom d'utilisateur (client envoie un formulaire et reçoit une confirmation), etc.

### 2. Quelles limites apparaissent si l’on veut une vraie messagerie “vivante” ?

Il n'y a pas de mise à jour automatique du serveur donc il faut faire des requêtes constantes et interroger le serveur régulièrement (polling) pour recevoir les nouveaux messages par exemple. 

### 3. Quelle solution pourrait-on introduire ensuite ?

Le projet utilise actuellement WebSocket pour permettre une connexion bidirectionnelle entre le client et le serveur. Le serveur peut envoyer des infos au client sans que le client ait a faire de requête: ça ajoute du "temps réel".