'use strict';
const {
  Model
} = require('sequelize');
module.exports = (sequelize, DataTypes) => {
  class organisations extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate(models) {
      // define association here
    }
  }
  organisations.init({
    organisation_id: {
      type: DataTypes.INTEGER,
      allowNull: false,
      validate: {
        notNull: {
          msg: 'organisation_id must have a value'
        },
        notEmpty: {
          msg: 'organisations_id must have a value'
        }
      }
    },
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
    }
  }, {
    sequelize,
    modelName: 'organisations',
  });
  return organisations;
};