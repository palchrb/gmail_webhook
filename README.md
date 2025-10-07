# gmail_webhook
Apps script code to give push based webhooks from gmail; subscribe/unsubscribe via API endpoint by passing a + alias and a webhook (and bearer auth if you want it)

Project is a pure copy paste to your apps script; need to copy both files, and establish a and connect a cloud project for it - where you can enable pub/sub to give push notifications for every new email.

Updated now with FIFO and automatic deletion once it receives 200 OK from webhook receiver.

What i might implement; media forwarding   

