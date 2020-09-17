const AJAX = function () {
    this.query = function (url, options) {
        return new Promise((resolve, reject) => {
            fetch(url, options)
                .then(response => {
                    if (response.status !== 200) {
                        reject({
                            message: response.statusText,
                            code: response.status
                        });
                    }
                    resolve(response.json());
                })
                .catch((ex) => {
                    reject({ error: ex });
                });
        });
    }
}
