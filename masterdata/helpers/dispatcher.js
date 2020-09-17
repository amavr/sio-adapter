'use strict';

module.exports = class Dispatcher {

    /// context - как правило - это "this" для получения прочих свойств и методов
    /// list - массив 
    /// func - async (context, item, i) => {return newItem;}
    /// limit - кол-во параллельно запущенных функций
    ///
    /// returns promises for analyses
    static async exec(context, list, func, limit = 10) {
        /// набор исполняемых операций
        let runned = new Set();

        return list.map(async (item, i) => {
            /// гонка промисов, которые добавлены в набор 
            /// по-сути ожидание, когда один из набора завершится,
            /// чтобы вместо него запустить следующий
            while (runned.size >= limit) {
                await Promise.race(runned);
            }

            /// запуск операции
            const promise = func(context, item, i);
            // добавляем в набор исполняемых
            runned.add(promise);
            //  ожидание заверения
            const res = await promise;
            // удаляем из набора исполняемых
            runned.delete(promise);

            return res;
        });
    }
    
}
