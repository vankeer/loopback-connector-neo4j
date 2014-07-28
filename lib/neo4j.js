'use strict';

var neo4j = require('node-neo4j');

/**
 * @module loopback-connector-neo4j
 *
 * @param dataSource The loopback-datasource-juggler dataSource
 * @param cb The callback function
 */
exports.initialize = function initializeDataSource(dataSource, cb) {
    if (!neo4j) {
        return;
    }
    var settings = dataSource.settings;
    var db = new neo4j(settings.neo4j_url);
    dataSource.connector = new Neo4jConnector(db);
    dataSource.db = db;
    cb && cb();
};

/**
 * @constructor
 * @param {Object} db
 */
function Neo4jConnector (db) {
    this.db = db;
}

Neo4jConnector.prototype.query = function query(query, params, include_stats, cb) {
    this.db.cypherQuery(query, params, include_stats, cb);
};

Neo4jConnector.prototype.create = function create(model, data, cb) {
    var neo = this;
    for (var key in data) {
        if (data.hasOwnProperty(key)) {
            // TODO not arrays
            if (typeof data[key] === 'object' || typeof data[key] === 'function')
                data[key] = JSON.stringify(data[key]);
        }
    }
    neo.db.insertNode(data, model, function insertNodeResult(err, node) {
        if (err || !node)
            cb('failed to add node!');
        else {
            cb(null, node);
        }
    });
};

Neo4jConnector.prototype.update = function update(model, where, data, cb) {
    var neo = this;
    var query = 'MATCH (n:`' + model + '`)';
    if (where) {
        query += ' WHERE' + buildParamList(where, '_where');
    }
    query += ' SET' + buildParamList(data);
    query += ' RETURN n';
    for (var key in where)
        data[key+'_where'] = where[key];
    this.query(query, data, false, function updateResult(err, r) {
        if (err)
            cb(err);
        else {
            // TODO no response is sent!
            if (r && r.data)
                cb(null, {updated: r.data});
            else
                cb('not found!');
        }
    });
};

// untested:
Neo4jConnector.prototype.updateOrCreate = Neo4jConnector.prototype.save = function save(model, data, cb) {
    var neo = this;
    for (var key in data)
    {
        if (data.hasOwnProperty(key)) {
            if (typeof data[key] === 'object' || typeof data[key] === 'function')
                data[key] = JSON.stringify(data[key]);
        }
    }
    if (!data._id)
        return cb('missing id!');
    neo.find(model, data._id, function findResult(err, node) {
        neo.db.updateNodesWithLabelsAndProperties(model, node, data, false, false, cb);
    });
};

function buildParamList(conds, addToParam) {
    if (conds === null || conds === undefined || (typeof conds !== 'object')) {
        return '';
    }
    var list = '';
    for (var property in conds) {
        if (typeof conds[property] === 'object')
            continue; // TODO add support for {op: value}
        // TODO add support for and/or
        var param = addToParam ? property + addToParam : property;
        list += ' n.' + property + ' = {' + param + '},';
    }
    list = list.slice(0, -1); // TODO check if conds empty
    return list;
}

function buildOrderBy(order) {
    if (typeof order === 'string') {
        order = [order];
    }
    return 'ORDER BY ' + order.map(function (o) {
        var t = o.split(/[\s,]+/);
        if (t.length === 1) {
            return o;
        }
        return t[0] + ' ' + t[1];
    }).join(', ');
}

function buildLimit(limit, offset) {
    if (isNaN(limit)) {
        limit = 0;
    }
    if (isNaN(offset)) {
        offset = 0;
    }
    return 'LIMIT ' + (offset ? (offset + ',' + limit) : limit);
}

Neo4jConnector.prototype.all = function all(model, filter, cb) {
    var neo = this;
    var query = 'MATCH (n:`' + model + '`)';
    var where = null;
    if (filter && filter.where) {
        where = filter.where;
        query += ' WHERE' + buildParamList(where);
    }
    query += ' RETURN n';
    if (filter) {
        if (filter.order) {
            query += ' ' + buildOrderBy(filter.order);
        }
        if (filter.limit) {
            query += ' ' + buildLimit(filter.limit, filter.skip || filter.offset || 0);
        }
    }
    neo.query(query, where, false, function queryResult(err, r) {
        if (err)
            cb(err);
        else {
            if (r.data)
                cb(null, r.data);
            else
                cb(null, r);
        }
    });

};

Neo4jConnector.prototype.exists = Neo4jConnector.prototype.find = function find(model, id, cb) {
    var neo = this;
    neo.db.readNode(id, function (err, node) {
        if (err) return cb(err);
        cb(null, node);
    });
};

Neo4jConnector.prototype.destroy = function (model, id, cb) {
    var neo = this;
    neo.db.deleteNode(id, function destroyResult(err, r) {
        if (err)
            cb(err);
        else
            cb(null, {deleted: r});
    });
};

Neo4jConnector.prototype.destroyAll = function (model, cb) {
    var neo = this;
    neo.db.deleteNodesWithLabelsAndProperties(model, '', function destroyResult(err, r) {
        if (err)
            cb(err);
        else
            cb(null, {deleted: r});
    });
};

Neo4jConnector.prototype.count = function (model, cb, where) {
    var neo = this;
    var w = where ? ' WHERE' + buildParamList(where) : '';
    var query = 'MATCH (n:`' + model + '`)' + w + ' RETURN count(n)';
    neo.query(query, where, false, cb);
};

Neo4jConnector.prototype.updateAttributes = function (model, id, data, cb) {
    var neo = this;
    neo.db.updateNodeById(id, data, cb);
};
