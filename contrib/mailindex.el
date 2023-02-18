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

(defvar mailindex-headers-group-to-headers (make-hash-table :test 'equal)
  "Temporary storage for the headers provided by mailindex")


(defvar mailindex-unprintable-regex (regexp-opt (loop for i from 0 below 32 collect (string i))))

(defun mailindex-to-nov (id headers)
  (make-full-mail-header id
                         (replace-regexp-in-string mailindex-unprintable-regex " " (or (getf headers :subject) ""))
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

    (lexical-let* ((proc (open-network-stream "mailindex" nil
                                              mailindex-host
                                              mailindex-port))
                   (buffer (with-current-buffer (get-buffer-create " *mailindex result*")
                             (erase-buffer)
                             (current-buffer)))
                   (buffer-read-pos 2)  ; skip the opening (
                   (entries ())
                   (process-entry (lambda (entry)
                                    (let ((group-name (aref entry 0))
                                          (article-number
                                           (string-to-number (aref entry 1)))
                                          (score (aref entry 2))
                                          (headers (aref entry 3)))
                                      (push (mailindex-to-nov article-number headers)
                                            (gethash group-name mailindex-headers-group-to-headers))
                                      (vector group-name article-number score)))))

      (clrhash mailindex-headers-group-to-headers)

      (set-process-filter proc (lambda (proc output)
                                 ;; Load the output into our read buffer, discarding the leading "(" if this is our first chunk of output.
                                 (with-current-buffer buffer
                                   (goto-char (point-max))

                                   (when (> (length output) 0)
                                     (insert output))

                                  ;; read as many complete entries as we can
                                   (goto-char buffer-read-pos)
                                   (let (read-result)
                                     (condition-case nil
                                         (while (setq next-entry (read buffer))
                                           (push (funcall process-entry next-entry)
                                                 entries)
                                           (setq buffer-read-pos (point)))
                                       (error nil))))))

      (process-send-string proc search-string)

      ;; while the process hasn't finished or there's still stuff in our buffer...
      (unwind-protect
          (while (or (zerop (process-exit-status proc))
                     (with-current-buffer buffer
                       (goto-char buffer-read-pos)
                       (not (looking-at ")"))))
            (accept-process-output proc 0 200))
        (set-process-filter proc nil)
        (ignore-errors (kill-process proc))
        (with-current-buffer buffer (erase-buffer)))

      (vconcat entries))))


(defun mailindex-retrieve-headers (artlist artgroup)
  (destructuring-bind (method group) (if (string-match ":" artgroup)
                                         (split-string artgroup ":")
                                       (list nil artgroup))
    (with-current-buffer nntp-server-buffer
      (erase-buffer)
      (dolist (header (gethash group mailindex-headers-group-to-headers nil))
        (nnheader-insert-nov header))))
  'nov)


(defun mailindex-initialise ()
  ;; Use the header information that mailindex provides (saves Gnus having to
  ;; sniff around for headers)
  (setq nnir-retrieve-headers-override-function 'mailindex-retrieve-headers)

  (setq nnir-method-default-engines '((nnml . mailindex)))

  (add-to-list 'nnir-engines
               '(mailindex nnir-run-mailindex)))


(mailindex-initialise)


;;; Support for emacs 28
(defclass mailindex () ())

(cl-defmethod gnus-search-run-search ((engine mailindex) srv query groups)
  (setq foo (nnir-run-mailindex query srv groups)))




(provide 'mailindex)
;;; mailindex.el ends here
