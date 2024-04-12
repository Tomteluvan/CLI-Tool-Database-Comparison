'use strict';
/** @type {import('sequelize-cli').Migration} */
module.exports = {
  async up(queryInterface, Sequelize) {
    await queryInterface.createTable('measurements', {
      device_id: {
        type: Sequelize.UUID,
        allowNull: false
      },
      value: {
        type: Sequelize.DOUBLE,
        allowNull: false
      },
      type: {
        type: Sequelize.SMALLINT,
        allowNull: false
      },
      timestamp: {
        type: Sequelize.DATE,
        allowNull: false
      },
    });
  },
  async down(queryInterface, Sequelize) {
    await queryInterface.dropTable('measurements');
  }
};