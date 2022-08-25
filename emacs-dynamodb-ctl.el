;;; emacs-dynamodb-ctl.el ---              -*- lexical-binding: t; -*-

;; Copyright (C) 2022  rcmerci

;; Author: rcmerci <rcmerci@gmail.com>
;; Keywords: tools
;; Package: emacs

;; This file is part of GNU Emacs.

;; GNU Emacs is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; GNU Emacs is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with GNU Emacs.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:
;;; TODO: support scan with filter
;;; TODO: support query
;;; TODO: cache table list

;;; Code:
(require 'dash)
(require 'benchmark)

(defvar emacs-dynamodb-ctl-describe-item-buffer-name "*emacs-dynamodb-ctl-describe-item*")
(defvar emacs-dynamodb-ctl-buffer-name "*emacs-dynamodb-ctl*")
(defvar-local emacs-dynamodb-ctl-current-table-name nil)
(defvar-local emacs-dynamodb-ctl-next-token nil)
;; (defvar-local table-list nil)

;;; utils

(defsubst hash-table-keys (hash-table)
  "Return a list of keys in HASH-TABLE."
  (let ((keys '()))
    (maphash (lambda (k _v) (push k keys)) hash-table)
    keys))

;;;


(defun emacs-dynamodb-ctl-table-list ()
  (message "fetching table list ...")
  (seq-into (gethash "TableNames" (json-parse-string (shell-command-to-string "aws dynamodb list-tables"))) 'list))

(defun emacs-dynamodb-ctl-scan (table-name &optional starting-token max-items)
  (message (format "scanning table [%s] ..." table-name))
  (let ((cmd (->
	      (list
	       (list "aws dynamodb scan")
	       (when max-items (list "--max-items" (number-to-string max-items)))
	       (when starting-token (list "--starting-token" starting-token))
	       (list "--table-name" table-name))
	      -flatten
	      (string-join " "))))
    (with-temp-buffer
      (let ((elapse (benchmark-elapse (insert (shell-command-to-string cmd)))))
	(message (format "scanning table [%s] ... DONE(%f s)"
			 table-name elapse)))
      (if (search-backward "Requested resource not found" nil t)
	  (user-error "Requested resource not found: table-name %s" table-name)
	(progn (goto-char (point-min))
	       (json-parse-buffer))))))

(defun emacs-dynamodb-ctl-get-item
    (table-name partition-key-name partition-key-value &optional range-key-name range-key-value)
  (let* ((partition-key-value*
	  (let ((ht (make-hash-table)))
	    (cond
	     ((stringp partition-key-value)
	      (puthash "S" partition-key-value ht))
	     ((numberp partition-key-value)
	      (puthash "N" (number-to-string partition-key-value) ht)))
	    ht))
	 (range-key-value*
	  (let ((ht (make-hash-table)))
	    (cond
	     ((stringp range-key-value)
	      (puthash "S" range-key-value ht))
	     ((numberp range-key-value)
	      (puthash "N" (number-to-string range-key-value) ht)))
	    ht))
	 (key
	  (let ((ht (make-hash-table)))
	    (puthash partition-key-name partition-key-value* ht)
	    (when range-key-name (puthash range-key-name range-key-value* ht))
	    ht))
	 (cmd (->
	       (list
		(list "aws dynamodb get-item")
		(list "--table-name" table-name)
		(list "--key" (format "'%s'" (json-serialize key))))
	       -flatten
	       (string-join " ")
	       )))
    (propertize (shell-command-to-string cmd) 'table-name table-name 'key key)))

(defun emacs-dynamodb-ctl-describe-item (item)
  (with-current-buffer (get-buffer-create emacs-dynamodb-ctl-describe-item-buffer-name)
    (erase-buffer)
    (insert (json-serialize item))
    (json-mode)
    (json-pretty-print-buffer))
  (pop-to-buffer emacs-dynamodb-ctl-describe-item-buffer-name))


(defun emacs-dynamodb-ctl-describe-item-at-point ()
  (interactive)
  (let ((item (tabulated-list-get-id (point))))
    (emacs-dynamodb-ctl-describe-item item)))

(defun emacs-dynamodb-ctl-switch-table ()
  (interactive)
  (setq emacs-dynamodb-ctl-current-table-name (completing-read "Select Table: " (emacs-dynamodb-ctl-table-list) nil t))
  ;; clear old data
  (setq tabulated-list-format nil)
  (setq tabulated-list-entries nil)
  (setq emacs-dynamodb-ctl-next-token nil)
  (tabulated-list-revert))

(defun emacs-dynamodb-ctl-load-more ()
  (when (and emacs-dynamodb-ctl-current-table-name emacs-dynamodb-ctl-next-token)
    (let* ((scan-result (emacs-dynamodb-ctl-scan emacs-dynamodb-ctl-current-table-name emacs-dynamodb-ctl-next-token 30))
	   (items (gethash "Items" scan-result))
	   (next-token (gethash "NextToken" scan-result))
	   (headers (seq-map 'car tabulated-list-format)))
      (setq tabulated-list-entries
	    (seq-concatenate
	     'list
	     (butlast tabulated-list-entries)
	     (seq-map (lambda (item)
			(list item
			      (seq-into
			       (seq-map (lambda (h) (emacs-dynamodb-ctl-item-value->string (gethash h item))) headers)
			       'vector)))
		      items)))
      (if next-token
	  (progn
	    (setq emacs-dynamodb-ctl-next-token next-token)
	    (setq tabulated-list-entries
		  (seq-concatenate 'list
				   tabulated-list-entries
				   (vector (list "more" (seq-concatenate 'vector
									 ["Press <RET> load more items"]
									 (make-vector (- (seq-length headers) 1) "")))))))
	(setq emacs-dynamodb-ctl-next-token nil))
      (tabulated-list-print)
      (goto-char (point-max)))))

(defun emacs-dynamodb-ctl-RET ()
  (interactive)
  (if (equal "more" (tabulated-list-get-id (point)))
      (emacs-dynamodb-ctl-load-more)
    (emacs-dynamodb-ctl-describe-item-at-point)
    )
  )

(defvar emacs-dynamodb-ctl-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "RET") #'emacs-dynamodb-ctl-RET)
    (define-key map (kbd "g") #'tabulated-list-revert)
    (define-key map (kbd "s") #'emacs-dynamodb-ctl-switch-table)
    map))

(defun emacs-dynamodb-ctl-item-value->string (v)
  (if (not v)
      ""
    (let ((s (gethash "S" v))
	  (n (gethash "N" v)))
      (cond
       (s s)
       (n n)
       (t (json-serialize v))))))

(defun refresh-tabulated-list-entries ()
  (when (not emacs-dynamodb-ctl-current-table-name)
    (setq emacs-dynamodb-ctl-current-table-name (completing-read "Select Table: " (emacs-dynamodb-ctl-table-list) nil t))
    ;; clear old data
    (setq tabulated-list-format nil)
    (setq tabulated-list-entries nil)
    (setq emacs-dynamodb-ctl-next-token nil))
  (let* ((scan-result (emacs-dynamodb-ctl-scan emacs-dynamodb-ctl-current-table-name nil 30))
	 (items (gethash "Items" scan-result))
	 (next-token (gethash "NextToken" scan-result))
	 (headers (seq-uniq (seq-concatenate 'list
					     (seq-mapcat (lambda (item) (hash-table-keys item)) items)
					     (seq-map #'car tabulated-list-format)))))
    (setq tabulated-list-format (seq-into (seq-map (lambda (h) (list h 30 t)) headers) 'vector))
    (setq tabulated-list-entries
	  (seq-map (lambda (item)
		     (list item
			   (seq-into (seq-map
				      (lambda (h) (emacs-dynamodb-ctl-item-value->string (gethash h item)))
				      headers)
				     'vector)))
		   items))
    (if next-token
	(progn
	  (setq emacs-dynamodb-ctl-next-token next-token)
	  (setq tabulated-list-entries
		(seq-concatenate 'list
				 tabulated-list-entries
				 (vector (list "more" (seq-concatenate 'vector
								       ["Press <RET> load more items"]
								       (make-vector (- (seq-length headers) 1) "")))))))
      (setq emacs-dynamodb-ctl-next-token nil))
    (tabulated-list-init-header)))



(define-derived-mode emacs-dynamodb-ctl-mode tabulated-list-mode "DynamoCtl"
  "mode for emacs-dynamodb-ctl"
  (buffer-disable-undo)
  (kill-all-local-variables)
  (setq major-mode 'emacs-dynamodb-ctl-mode)
  (setq mode-name "DynamoCtl")
  (use-local-map emacs-dynamodb-ctl-mode-map)
  (add-hook 'tabulated-list-revert-hook #'refresh-tabulated-list-entries nil t))

(defun emacs-dynamodb-ctl ()
  (interactive)
  (pop-to-buffer (get-buffer-create emacs-dynamodb-ctl-buffer-name))
  (emacs-dynamodb-ctl-mode))

(provide 'emacs-dynamodb-ctl)
