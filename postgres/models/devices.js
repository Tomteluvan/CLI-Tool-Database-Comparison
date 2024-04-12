'use strict';
const {
  Model
} = require('sequelize');
module.exports = (sequelize, DataTypes) => {
  class devices extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate(models) {
      // define association here
    }
  }
  devices.init({
    id: {
      type: DataTypes.UUID,
      allowNull: false,
      primaryKey: true,
      validate: {
         notNull: {
          msg: 'id must have a value'
         },
         notEmpty: {
          msg: 'id must have a value'
         }
      } 
    },
    serial: {
      type: DataTypes.TEXT,
      allowNull: false,
      validate: {
         notNull: {
          msg: 'serial must have a value'
         },
         notEmpty: {
          msg: 'serial must have a value'
         }
      }
    },
    type: {
      type: DataTypes.TEXT,
      allowNull: false,
      validate: {
         notNull: {
          msg: 'type must have a value'
         },
         notEmpty: {
          msg: 'type must have a value'
         }
      }
    },
    sub_type: {
      type: DataTypes.TEXT,
      allowNull: false,
      validate: {
         notNull: {
          msg: 'sub_type must have a value'
         },
         notEmpty: {
          msg: 'sub_type must have a value'
         }
      }
    }
  }, {
    sequelize,
    modelName: 'devices',
  });
  return devices;
};