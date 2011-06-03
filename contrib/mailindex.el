;;; mailindex.el --- Integrate Gnus with mailindex

;; Copyright (C) 2011 Mark Triggs

;; Author: Mark Triggs <mark@dishevelled.net>
;; Keywords: gnus, mail, search, lucene

;; This program is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with this program.  If not, see <http://www.gnu.org/licenses/>.

;; Commentary

;; This file provides integration between Gnus's nnir.el and mailindex
;; (http://www.github.com/marktriggs/mailindex), a Lucene-based mail search
;; engine for Gnus.
;;
;; Using it should hopefully be straightforward.  Follow the README on Github to
;; get mailindex running and indexing your mail.  Then enable it in Gnus with:
;;
;;  (require 'mailindex)
;;
;;  (setq gnus-select-method '(nnml ""
;;                                  (nnir-search-engine mailindex)
;;                                  (mailindex-host "localhost")
;;                                  (mailindex-port 4321)))
;;
;;  (define-key gnus-group-mode-map (kbd "G G")
;;    'gnus-group-make-nnir-group)
;;
;;
;; Once you have done that, hitting "G G" in your Gnus group buffer should
;; search using mailindex and find some messages.
;;
;; By default, mailindex will search for your query string across all groups.
;; If you prefer the behaviour of only searching the group under the point (or
;; groups you've process marked), you can set `mailindex-search-all-groups' to
;; nil.

;;; Code:

(require 'nnir)
(require 'cl)

(defvar mailindex-search-all-groups t
  "If true, ignore the list of groups nnir passes and search all groups.
Otherwise, just search for the subset.")

(defvar mailindex-headers nil
  "Temporary storage for the headers provided by mailindex")


(defun mailindex-to-nov (id headers)
  (make-full-mail-header id
                         (getf headers :subject)
                         (getf headers :from)
                         (getf headers :date)
                         (or (getf headers :msgid)
                             (nnheader-generate-fake-message-id id))
                         nil
                         (getf headers :chars)
                         (getf headers :lines)
                         nil
                         nil))


(defun nnir-run-mailindex (query server &optional grouplist)
  (let* ((method (gnus-server-to-method server))
         (mailindex-host (cadr (assoc 'mailindex-host (cddr method))))
         (mailindex-port (cadr (assoc 'mailindex-port (cddr method))))
         (search-string (concat (cdr (assoc 'query query)) "\n")))

    (unless (and mailindex-host mailindex-port)
      (error (concat "You need to specify `mailindex-host' and "
                     "`mailindex-port' in your Gnus select method.")))

    (unless mailindex-search-all-groups
      (setq search-string
            (format "group:(%s) AND %s"
                    (mapconcat (lambda (s) (format "\"%s\"" s))
                               grouplist " OR ")
                    search-string)))

    (let ((proc (open-network-stream "mailindex" nil
                                     mailindex-host
                                     mailindex-port))
          (result ""))
      (set-process-filter proc (lambda (proc output)
                                 (setq result (concat result output))))
      (process-send-string proc search-string)
      (while (zerop (process-exit-status proc))
        (sit-for 0.1))
      (setq mailindex-headers '())
      (vconcat (mapcar (lambda (entry)
                         (let ((group-name (aref entry 0))
                               (article-number (string-to-number
                                                (aref entry 1)))
                               (score (aref entry 2))
                               (headers (aref entry 3)))
                           (push (cons group-name
                                       (mailindex-to-nov article-number
                                                         headers))
                                 mailindex-headers))
                         (vector
                          (concat server (aref entry 0))    ; group name
                          (string-to-number (aref entry 1)) ; article number
                          (aref entry 2)))                  ; score
                       (car (ignore-errors (read-from-string result))))))))


(defun mailindex-retrieve-headers (artlist artgroup)
  (destructuring-bind (method group) (split-string artgroup ":")
    (with-current-buffer nntp-server-buffer
      (erase-buffer)
      (loop for header in mailindex-headers
            when (string= (first header) group)
            do (nnheader-insert-nov (cdr header)))))
  'nov)


(defun mailindex-initialise ()
  ;; Use the header information that mailindex provides (saves Gnus having to
  ;; sniff around for headers)
  (setq nnir-retrieve-headers-override-function 'mailindex-retrieve-headers)

  (add-to-list 'nnir-engines
               '(mailindex nnir-run-mailindex)))


(mailindex-initialise)


(provide 'mailindex)
;;; mailindex.el ends here
