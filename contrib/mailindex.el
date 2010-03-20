;;; mailindex.el

;; Copyright (C) 2009  Mark Triggs

;; Author: Mark Triggs <mark@dishevelled.net>

;; This file is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation; either version 3, or (at your option)
;; any later version.

;; This file is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with GNU Emacs; see the file COPYING.  If not, write to
;; the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
;; Boston, MA 02110-1301, USA.

;;; Code:


(require 'nnir)
(require 'cl)

(defun nnir-run-mst-search (args server group)
  (let ((proc (open-network-stream "mailindex" nil "localhost" 4321))
        (result ""))
    (set-process-filter proc (lambda (proc output)
                               (setq result (concat result output))))
    (process-send-string proc (concat (cdr (assoc 'query args)) "\n"))
    (while (zerop (process-exit-status proc))
      (sit-for 0.1))
    (mapcar (lambda (entry)
              (vector
               (concat server (aref entry 0))
               (string-to-number (aref entry 1))
               (aref entry 2)
               (aref entry 3)))
            (car (ignore-errors (read-from-string result))))))


(defun nnir-run-mst-nov (art)
  (destructuring-bind (group id score options) (coerce art 'list)
    (make-full-mail-header id
                           (getf options :subject)
                           (getf options :from)
                           (getf options :date)
                           (or (getf options :msgid)
                               (nnheader-generate-fake-message-id id))
                           nil
                           (getf options :chars)
                           (getf options :lines)
                           nil
                           nil)))


(setq nnir-get-article-nov-override-function 'nnir-run-mst-nov)

(push '(mst nnir-run-mst-search nil)
      nnir-engines)

(setq nnir-search-engine 'mst)




(provide 'mailindex)
;;; mailindex.el ends here
