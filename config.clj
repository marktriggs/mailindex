{:indexfile "/mnt/ssd/mst-home/mail-index"
 :port 4321
 :reindex-frequency 60000
 :max-results 10000
 :optimize-hour 4                       ; optimise at 4am
 :backends [{:name "gnus-mail"
             :backend net.dishevelled.mailindex.backends.nnml
             :base "/home/mst/.mail"}]}
