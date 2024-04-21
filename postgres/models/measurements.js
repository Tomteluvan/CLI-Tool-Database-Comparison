'use strict';
const {
  Model
} = require('sequelize');
module.exports = (sequelize, DataTypes) => {
  class measurements extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate(models) {
      // define association here
    }
  }
  measurements.init({
    device_id: {
      type: DataTypes.UUID,
      allowNull: false,
      validate: {
        notNull: {
          msg: 'device_id must have a value'
        },
        notEmpty: {
          msg: 'device_id must have a value'
        }
      }
    },
    value: {
      type: DataTypes.DOUBLE,
      allowNull: false,
      validate: {
        notNull: {
          msg: 'value must have a value'
        },
        notEmpty: {
          msg: 'value must have a value'
        }
      }
    },
    type: {
      type: DataTypes.SMALLINT,
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
    timestamp: {
      type: DataTypes.DATE,
      allowNull: false,
      notNull: {
        msg: 'timestamp must have a value'
      }, 
      notEmpty: {
        msg: 'timestamp must have a value'
      }
    }
  }, {
    sequelize,
    modelName: 'measurements',
  });
  return measurements;
};