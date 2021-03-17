'use strict';

const array_routes = require('../resources/const.json').ARRAY_ROUTES;

module.exports = class Adapter {

    /**
     * Преобразование (рекурсивное) JSON иерархии в однообразную структуру
     * 
     * @param {*} node узел для обработки
     * @return {*} иерархическая структура
     * 
     * @static
     */
    static getFields(node) {
        const base = [];
        const kids = [];
        for (let key in node) {
            if (key !== 'nodes') {
                base.push({ name: key, val: node[key] });
            }
            else {
                node.nodes.forEach(item => {
                    kids.push(Adapter.getFields(item))
                });
            }
            // console.log(key);
        }
        return { base: base, kids: kids };
    }

    /**
     * Получить значение вложенного узла по маршруту
     * 
     * @param {*} node       - родительский узел
     * @param {string} route - маршрут
     * @param {*} defVal - значение по умолчанию
     * 
     * @static
     */
    static getVal(node, route, defVal) {
        let val = null;
        if (defVal === undefined) defVal = null;
        try {
            const keys = route.split('/').filter(key => key.length > 0);
            const len = keys.length;
            if (len === 0) return defVal;
            if (len === 1) {
                val = node[route];
            }
            else {
                for (let i = 0; i < len; i++) {
                    node = node[keys[i]];
                }
                val = node;
            }
        }
        catch (ex) {
            // console.log(ex);
            // console.log(route);
        }
        return val === undefined ? defVal : val;
    }

    static getNodes(node, route) {
        try {
            return route.split('/').reduce((items, route_segment) => {
                let nodes = [];
                items.map((item) => {
                    const node = item[route_segment];
                    if(node){
                        nodes = [...nodes, ...this.nodeAsArray(node)];
                    }
                });
                return nodes;
            }, [node]);

        }
        catch (ex) {
            return [];
            // console.log(ex);
            // console.log(route);
        }
    }

    /**
     * Преобразование узла в массив с одним элементом
     * 
     * @param {*} node 
     */
    static nodeAsArray(node) {
        const nodes = [];
        if (node) {
            if (Array.isArray(node)) {
                return node;
            }
            nodes.push(node);
        }
        return nodes;
    }

    /**
     * Преобразование указанных в маршрутах узлов в массивы
     * 
     * @param {*} parentNode - родительский узел
     * @param {*} parentRoute - маршрут родителького узла
     * @param {string[]} arrayRoutes - маршруты дочерних узлов
     */
    static normalize(parentNode, parentRoute, arrayRoutes) {

        const is_array = Array.isArray(parentNode);
        const is_object = typeof parentNode === 'object';

        /// если не массив и не объект, то подчиненных узлов нет
        if (!is_array && !is_object) return;

        for (let key in parentNode) {
            const node = parentNode[key];
            /// если узел - это массив, то его индексы не учитываются в маршруте 
            const route = is_array ? parentRoute : parentRoute + '/' + key.toLowerCase();
            /// и не должны преобразовываться в массив
            const matched = !is_array && arrayRoutes.includes(route);
            if (matched) {
                // console.log(route, matched);

                /// если это должен быть массив, но не массив
                if (!Array.isArray(node)) {
                    parentNode[key] = Adapter.nodeAsArray(node);
                }
            }

            Adapter.normalize(node, route, arrayRoutes);
        }
    }

    /**
     * Преробразовывает объекты по указанным маршрутам в массивы объектов
     * 
     * @param {*} doc JSON Документ, который надо нормализовать
     */
    static prepareArrays(doc) {
        Adapter.normalize(doc, '', array_routes.map(item => item.toLowerCase()));
    }

}