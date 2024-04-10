'use strict';
/** @type {import('sequelize-cli').Migration} */
module.exports = {
  async up(queryInterface, Sequelize) {
    await queryInterface.createTable('organisations', {
      organisation_id: {
        type: Sequelize.INTEGER,
        allowNull: false
      },
      device_id: {
        type: Sequelize.UUID,
        allowNull: false
      }
    });
  },
  async down(queryInterface, Sequelize) {
    await queryInterface.dropTable('organisations');
  }
};